package nntpPool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/estorepe/nntp"
	"sync"
	"time"
)

var (
	WarnChan   = make(chan error, 10) // Warning messages (errors which did not cause the pool to fail)
	LogChan    = make(chan string, 10) // Informative messages
	DebugChan  = make(chan string, 10) // Additional debug messages
)

var (
	errMaxConnsShouldBePositive = errors.New("max conns should be greater than 0")
	errConsIsGreaterThanMax     = errors.New("initial amount of connections should be lower than or equal to max conns")
	errPoolWasClosed            = errors.New("connection pool was closed")
	errConnIsNil                = errors.New("connection is nil")
)

type ConnectionPool interface {
	Conns() (uint32, uint32)
	MaxConns() uint32
	Get(ctx context.Context) (*NNTPConn, error)
	Put(conn *NNTPConn)
	Close()
}

type Config struct {
	Name                  string
	Host                  string
	Port                  uint32
	SSL                   bool
	SkipSSLCheck          bool
	User                  string
	Pass                  string
	MaxConns              uint32
	ConnWaitTime          time.Duration
	IdleTimeout           time.Duration
	HealthCheck           bool
	MaxTooManyConnsErrors uint32
	MaxConnErrors         uint32
}

type NNTPConn struct {
	*nntp.Conn
	timestamp time.Time
	closed    bool
}

type connectionPool struct {
	connsMutex           sync.RWMutex
	connsChan            chan NNTPConn
	name                 string
	host                 string
	port                 uint32
	ssl                  bool
	skipSslCheck         bool
	user                 string
	pass                 string
	maxConns             uint32
	connWaitTime         time.Duration
	idleTimeout          time.Duration
	healthCheck          bool
	maxTooManyConnsErrors uint32
	maxConnErrors        uint32
	conns                uint32
	connAttempts         uint32
	closed               bool
	fatalError           error
	serverLimit          uint32
	tooManyConnsErrors   uint32
	connErrors           uint32
	startupWG            sync.WaitGroup
	created              bool
	maxConnsUsed         uint32
}

func New(cfg *Config, initialConns uint32) (ConnectionPool, error) {
	if cfg.MaxConns <= 0 {
		return nil, errMaxConnsShouldBePositive
	}
	if initialConns > cfg.MaxConns {
		return nil, errConsIsGreaterThanMax
	}

	pool := &connectionPool{
		connsChan:             make(chan NNTPConn, cfg.MaxConns),
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
		serverLimit:           cfg.MaxConns,
	}

	for i := uint32(0); i < initialConns; i++ {
		pool.startupWG.Add(1)
		go pool.addConn()
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

func (cp *connectionPool) Get(ctx context.Context) (*NNTPConn, error) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()

	defer func() {
		usedConns := cp.conns - uint32(len(cp.connsChan))
		if usedConns > cp.maxConnsUsed {
			cp.maxConnsUsed = usedConns
		}
	}()

	for {
		if cp.fatalError != nil {
			cp.Close()
			return nil, cp.fatalError
		}
		select {
		case <-ctx.Done():
			cp.Close()
			return nil, ctx.Err()
		case conn, ok := <-cp.connsChan:
			if !ok {
				if cp.fatalError != nil {
					return nil, cp.fatalError
				}
				return nil, errPoolWasClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			if err := cp.warmConnection(&conn); err != nil {
				cp.error(err)
				continue
			}
			return &conn, nil
		default:
			if cp.conns < cp.serverLimit {
				go cp.addConn()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (cp *connectionPool) warmConnection(conn *NNTPConn) error {
	_, err := conn.Date()
	return err
}

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

func (cp *connectionPool) Close() {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		return
	}
	cp.debug("closing pool")
	close(cp.connsChan)
	for conn := range cp.connsChan {
		conn.close()
	}
	cp.closed = true
	cp.conns = 0
	cp.connAttempts = 0
	cp.debug("pool closed")
}

func (cp *connectionPool) Conns() (uint32, uint32) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		return 0, 0
	}
	conns := cp.conns
	return conns - uint32(len(cp.connsChan)), conns
}

func (cp *connectionPool) MaxConns() uint32 {
	return cp.maxConnsUsed
}

func (cp *connectionPool) factory() (*nntp.Conn, error) {
	var conn *nntp.Conn
	var err error
	if cp.ssl {
		sslConfig := tls.Config{InsecureSkipVerify: cp.skipSslCheck}
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

	if err = conn.EnablePipelining(); err != nil {
		cp.debug("Pipelining not supported: " + err.Error())
	}

	return conn, nil
}

func (cp *connectionPool) addConn() {
	cp.connsMutex.Lock()
	if cp.closed {
		cp.connsMutex.Unlock()
		return
	}
	if cp.connAttempts >= cp.serverLimit {
		cp.debug(fmt.Sprintf("ignoring new connection attempt (current connection attempts: %v | current server limit: %v connections)", cp.connAttempts, cp.serverLimit))
		cp.connsMutex.Unlock()
		return
	}

	cp.connAttempts++
	cp.connsMutex.Unlock()
	conn, err := cp.factory()
	cp.connsMutex.Lock()

	abort := func() {
		cp.connAttempts--
		if conn != nil {
			conn.Quit()
		}
	}

	if cp.closed {
		abort()
		cp.connsMutex.Unlock()
		return
	}

	if err != nil {
		cp.error(err)
		abort()
		if cp.maxTooManyConnsErrors > 0 && (err.Error()[0:3] == "482" || err.Error()[0:3] == "502") && cp.conns > 0 {
			cp.tooManyConnsErrors++
			if cp.tooManyConnsErrors >= cp.maxTooManyConnsErrors && cp.serverLimit > cp.conns {
				cp.serverLimit = cp.conns
				cp.error(fmt.Errorf("reducing max connections to %v due to repeated 'too many connections' error", cp.serverLimit))
			}
		} else {
			if cp.maxConnErrors > 0 {
				cp.connErrors++
				if cp.connErrors >= cp.maxConnErrors && cp.conns == 0 {
					cp.fatalError = fmt.Errorf("unable to establish a connection - last error was: %v", err)
					cp.connsMutex.Unlock()
					cp.error(cp.fatalError)
					cp.Close()
					return
				}
			}
		}
		cp.connsMutex.Unlock()
		go func() {
			cp.debug(fmt.Sprintf("waiting %v seconds for next connection retry", cp.connWaitTime))
			time.Sleep(cp.connWaitTime)
			cp.addConn()
		}()
		return
	}

	cp.tooManyConnsErrors = 0
	cp.connErrors = 0

	select {
	case cp.connsChan <- NNTPConn{Conn: conn, timestamp: time.Now()}:
		cp.conns++
		cp.debug(fmt.Sprintf("new connection opened (%v of %v connections available)", cp.conns, cp.serverLimit))
	default:
		abort()
	}
	cp.connsMutex.Unlock()
}

func (cp *connectionPool) closeConn(conn *NNTPConn) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if !conn.closed {
		cp.conns--
		cp.connAttempts--
		conn.close()
		cp.debug(fmt.Sprintf("connection closed (%v of %v connections available)", cp.conns, cp.serverLimit))
	}
}

func (cp *connectionPool) checkConnIsHealthy(conn NNTPConn) bool {
	if cp.idleTimeout > 0 && conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) {
		cp.debug("closing expired connection")
		cp.closeConn(&conn)
		return false
	}
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
			go c.Quit()
		}
		c.closed = true
	}
}

func (c *NNTPConn) ping() error {
	if c.Conn != nil {
		_, err := c.Date()
		return err
	}
	return errConnIsNil
}
