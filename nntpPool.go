package nntpPool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tensai75/nntp"
)

var (
	// message channels
	WarnChan  = make(chan error, 10)  // warning messages (errors which did not cause the pool to fail)
	LogChan   = make(chan string, 10) // informative messages
	DebugChan = make(chan string, 10) // additional debug messages
)

var (
	errMaxConnsShouldBePositive = errors.New("max conns should be greater than 0")
	errConsIsGreaterThanMax     = errors.New("initial amount of connections should be lower than or equal to max conns")
	errPoolWasClosed            = errors.New("connection pool was closed")
	errConnIsNil                = errors.New("connection is nil")
)

type ConnectionPool interface {
	// Returns number of currently used and total opened connections.
	Conns() (uint32, uint32)
	// Returns the maximum number of simultaneously used connections.
	MaxConns() uint32
	// Retrieves connection from pool if it exists or opens new connection.
	Get(ctx context.Context) (*NNTPConn, error)
	// Returns connection to pool.
	Put(conn *NNTPConn)
	// Closes all connections and pool.
	Close()
}

type Config struct {
	// Name for the usenet server
	Name string
	// Usenet server host name or IP address
	Host string
	// Usenet server port number
	Port uint32
	// Use SSL if set to true
	SSL bool
	// Skip SSL certificate check
	SkipSSLCheck bool
	// Username to connect to the usenet server
	User string
	// Password to connect to the usenet server
	Pass string
	// Max number of opened connections.
	MaxConns uint32
	// Time to wait in seconds before trying to re-connect
	ConnWaitTime time.Duration
	// Duartion after idle connections will be closed
	IdleTimeout time.Duration
	// Check health of connection befor passing it on
	HealthCheck bool
	// Number of max "too many connections" errors after which MaxConns is automatically reduced (0 = disabled)
	MaxTooManyConnsErrors uint32
	// Number of max consecutive connection errors after which the pool fails if no connection could be established at all (0 = disabled)
	MaxConnErrors uint32
}

type NNTPConn struct {
	*nntp.Conn
	timestamp time.Time
	closed    bool
}

type connectionPool struct {
	connsMutex sync.RWMutex
	connsChan  chan NNTPConn

	name                  string
	host                  string
	port                  uint32
	ssl                   bool
	skipSslCheck          bool
	user                  string
	pass                  string
	maxConns              uint32
	connWaitTime          time.Duration
	idleTimeout           time.Duration
	healthCheck           bool
	maxTooManyConnsErrors uint32
	maxConnErrors         uint32

	conns              uint32
	connAttempts       uint32
	closed             bool
	fatalError         error
	serverLimit        uint32
	tooManyConnsErrors uint32
	connErrors         uint32
	startupWG          sync.WaitGroup
	created            bool
	maxConnsUsed       uint32
}

// Opens new connection pool.
func New(cfg *Config, initialConns uint32) (ConnectionPool, error) {
	switch {
	case cfg.MaxConns <= 0:
		return nil, errMaxConnsShouldBePositive
	case initialConns > cfg.MaxConns:
		return nil, errConsIsGreaterThanMax
	}

	pool := &connectionPool{
		connsMutex: sync.RWMutex{},
		connsChan:  make(chan NNTPConn, cfg.MaxConns),

		name:                  cfg.Name,
		host:                  cfg.Host,
		port:                  cfg.Port,
		ssl:                   cfg.SSL,
		skipSslCheck:          cfg.SkipSSLCheck,
		user:                  cfg.User,
		pass:                  cfg.Pass,
		maxConns:              cfg.MaxConns,
		connWaitTime:          cfg.ConnWaitTime,
		idleTimeout:           cfg.IdleTimeout,
		healthCheck:           cfg.HealthCheck,
		maxTooManyConnsErrors: cfg.MaxTooManyConnsErrors,
		maxConnErrors:         cfg.MaxConnErrors,

		closed:      false,
		fatalError:  nil,
		serverLimit: cfg.MaxConns,
	}

	for i := uint32(0); i < initialConns; i++ {
		pool.startupWG.Add(1)
		pool.addConn()
	}
	pool.startupWG.Wait()
	if pool.fatalError != nil {
		pool.Close()
		return nil, pool.fatalError
	}
	pool.created = true
	if initialConns > 0 && pool.conns < initialConns {
		pool.log(fmt.Sprintf("pool created with errors (%v of %v requested connections available)", pool.conns, initialConns))
	} else {
		pool.log("pool created successfully")
	}
	return pool, nil
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool) Get(ctx context.Context) (*NNTPConn, error) {
	for {
		cp.connsMutex.RLock()
		if cp.fatalError != nil {
			cp.connsMutex.RUnlock()
			cp.Close()
			return nil, cp.fatalError
		}
		cp.connsMutex.RUnlock()

		select {
		case <-ctx.Done():
			cp.Close()
			return nil, ctx.Err()

		case conn, ok := <-cp.connsChan:
			if !ok {
				cp.connsMutex.RLock()
				if cp.fatalError != nil {
					cp.connsMutex.RUnlock()
					return nil, cp.fatalError
				}
				cp.connsMutex.RUnlock()
				return nil, errPoolWasClosed
			}
			if cp.checkConnIsHealthy(conn) {
				return &conn, nil
			}

		default:
			cp.connsMutex.RLock()
			if cp.connAttempts < cp.serverLimit && !cp.closed {
				cp.connsMutex.RUnlock()
				go cp.addConn()
				continue
			}
			cp.connsMutex.RUnlock()

			conn, ok := <-cp.connsChan
			if !ok {
				cp.connsMutex.RLock()
				if cp.fatalError != nil {
					cp.connsMutex.RUnlock()
					return nil, cp.fatalError
				}
				cp.connsMutex.RUnlock()
				return nil, errPoolWasClosed
			}
			if cp.checkConnIsHealthy(conn) {
				return &conn, nil
			}
		}
	}
}
// Returns connection to the pool.
func (cp *connectionPool) Put(conn *NNTPConn) {
	cp.connsMutex.Lock()
	if !cp.closed {
		select {
		case cp.connsChan <- NNTPConn{Conn: conn.Conn, timestamp: time.Now()}:
			cp.connsMutex.Unlock()
			return
		default:
			cp.debug("closing returned connection because pool is full")
		}
	} else {
		cp.debug("closing returned connection because pool is closed")
	}
	cp.connsMutex.Unlock()
	cp.closeConn(conn)
}

// Closes all connections and pool.
func (cp *connectionPool) Close() {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		// pool is already closed
		return
	}
	cp.debug("closing pool")
	if cp.connsChan != nil {
		cp.debug("closing connection channel")
		close(cp.connsChan)
		cp.debug("closing open connections")
		for conn := range cp.connsChan {
			conn.close()
		}
	}
	cp.closed = true
	cp.conns = 0
	cp.connAttempts = 0
	cp.debug("pool closed")
}

// Returns number of opened connections.
func (cp *connectionPool) Conns() (uint32, uint32) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		return 0, 0
	} else {
		conns := cp.conns
		return conns - uint32(len(cp.connsChan)), conns
	}
}

// Returns number of max used connections.
func (cp *connectionPool) MaxConns() uint32 {
	return cp.maxConnsUsed
}

// factory function for the new nntp connections
func (cp *connectionPool) factory() (*nntp.Conn, error) {
	var conn *nntp.Conn
	var err error
	if cp.ssl {
		sslConfig := tls.Config{
			InsecureSkipVerify: cp.skipSslCheck,
		}
		conn, err = nntp.DialTLS("tcp", fmt.Sprintf("%v:%v", cp.host, cp.port), &sslConfig)
	} else {
		conn, err = nntp.Dial("tcp", fmt.Sprintf("%v:%v", cp.host, cp.port))
	}
	if err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	if err = conn.Authenticate(cp.user, cp.pass); err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	return conn, nil
}

func (cp *connectionPool) addConn() {
	go func() {
		defer func() {
			if !cp.created {
				cp.startupWG.Done()
			}
		}()

		cp.connsMutex.Lock()
		if cp.closed {
			// pool is already closed
			cp.connsMutex.Unlock()
			return
		}
		if cp.connAttempts >= cp.serverLimit {
			// ignoring the connection attempt if there are already too many attempts
			cp.debug(fmt.Sprintf("ignoring new connection attempt (current connection attempts: %v | current server limit: %v connections)", cp.connAttempts, cp.serverLimit))
			cp.connsMutex.Unlock()
			return
		}

		// try to open the connection
		cp.connAttempts++
		cp.connsMutex.Unlock()
		conn, err := cp.factory()
		cp.connsMutex.Lock()

		// abort function for reducing conAttempts counter and closing the connection
		abort := func() {
			cp.connAttempts--
			if conn != nil {
				conn.Quit()
			}
		}

		if cp.closed {
			// if pool was closed meanwhile, abort
			abort()
			cp.connsMutex.Unlock()
			return
		}

		// connection error
		if err != nil {
			cp.error(err)
			// abort and handle error
			abort()
			if cp.maxTooManyConnsErrors > 0 && (err.Error()[0:3] == "482" || err.Error()[0:3] == "502") && cp.conns > 0 {
				// handle too many connections error
				cp.tooManyConnsErrors++
				if cp.tooManyConnsErrors >= cp.maxTooManyConnsErrors && cp.serverLimit > cp.conns {
					cp.serverLimit = cp.conns
					cp.error(fmt.Errorf("reducing max connections to %v due to repeated 'too many connections' error", cp.serverLimit))
				}
			} else {
				// handle any other error
				if cp.maxConnErrors > 0 {
					cp.connErrors++
					if cp.connErrors >= cp.maxConnErrors && cp.conns == 0 {
						cp.fatalError = fmt.Errorf("unable to establish a connection  - last error was: %v", err)
						cp.connsMutex.Unlock()
						cp.error(cp.fatalError)
						cp.Close()
						return
					}
				}
			}
			cp.connsMutex.Unlock()
			// retry to connect
			go func() {
				cp.debug(fmt.Sprintf("waiting %v seconds for next connection retry", cp.connWaitTime))
				time.Sleep(cp.connWaitTime)
				cp.addConn()
			}()
			return
		}

		// connection successfull
		cp.tooManyConnsErrors = 0
		cp.connErrors = 0

		select {
		// try to push connection to the connections channel
		case cp.connsChan <- NNTPConn{
			Conn:      conn,
			timestamp: time.Now(),
		}:
			cp.conns++
			cp.debug(fmt.Sprintf("new connection opened (%v of %v connections available)", cp.conns, cp.serverLimit))

		// if the connection channel is full, abort
		default:
			abort()
		}
		cp.connsMutex.Unlock()
	}()
}

func (cp *connectionPool) closeConn(conn *NNTPConn) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	cp.conns--
	cp.connAttempts--
	conn.close()
	cp.debug(fmt.Sprintf("connection closed (%v of %v connections available)", cp.conns, cp.serverLimit))
}

func (cp *connectionPool) checkConnIsHealthy(conn NNTPConn) bool {
	// closing expired connection
	if cp.idleTimeout > 0 &&
		conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) {
		cp.debug("closing expired connection")
		cp.closeConn(&conn)
		return false
	}
	// closing unhealthy connection
	if cp.healthCheck {
		if err := conn.ping(); err != nil {
			cp.debug("closing unhealthy connection")
			cp.closeConn(&conn)
			return false
		}
	}
	return true
}

func (cp *connectionPool) log(text string) {
	select {
	case LogChan <- fmt.Sprintf("%s: %s", cp.name, text):
	default:
	}
}

func (cp *connectionPool) error(err error) {
	select {
	case WarnChan <- fmt.Errorf("%s: %v", cp.name, err):
	default:
	}
}

func (cp *connectionPool) debug(text string) {
	select {
	case DebugChan <- fmt.Sprintf("%s: %v", cp.name, text):
	default:
	}
}

func (c *NNTPConn) close() {
	if !c.closed {
		if c.Conn != nil {
			go c.Conn.Quit()
		}
		c.closed = true
	}
}

func (c *NNTPConn) ping() error {
	if c.Conn != nil {
		_, err := c.Conn.Date()
		return err
	} else {
		return errConnIsNil
	}
}
