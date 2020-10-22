package amqpextra

import (
	"context"
	"errors"
	"sync"
	"time"

	"fmt"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type Option func(c *Dialer)

type AMQPConnection interface {
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Close() error
}

type Connection struct {
	amqpConn AMQPConnection
	lostCh   chan struct{}
	closeCh  chan struct{}
}

func (c *Connection) AMQPConnection() *amqp.Connection {
	return c.amqpConn.(*amqp.Connection)
}

func (c *Connection) NotifyLost() chan struct{} {
	return c.lostCh
}

func (c *Connection) NotifyClose() chan struct{} {
	return c.closeCh
}

type config struct {
	amqpUrls   []string
	amqpDial   func(url string, c amqp.Config) (AMQPConnection, error)
	amqpConfig amqp.Config

	logger      logger.Logger
	retryPeriod time.Duration
	ctx         context.Context
}

type Dialer struct {
	config

	ctx        context.Context
	cancelFunc context.CancelFunc

	connCh chan *Connection

	mu         *sync.Mutex
	readyChs   []chan struct{}
	unreadyChs []chan error
	closedCh   chan struct{}
}

func Dial(opts ...Option) (*amqp.Connection, error) {
	d, err := NewDialer(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()

	return d.Connection(ctx)
}

func NewDialer(opts ...Option) (*Dialer, error) {
	c := &Dialer{
		config: config{
			amqpUrls: make([]string, 0, 1),
			amqpDial: func(url string, c amqp.Config) (AMQPConnection, error) {
				return amqp.DialConfig(url, c)
			},
			amqpConfig: amqp.Config{
				Heartbeat: time.Second * 30,
				Locale:    "en_US",
			},

			retryPeriod: time.Second * 5,
			logger:      logger.Discard,
		},

		mu: new(sync.Mutex),

		connCh:   make(chan *Connection),
		closedCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	for _, unreadyCh := range c.unreadyChs {
		if unreadyCh == nil {
			return nil, errors.New("unready chan must be not nil")
		}

		if cap(unreadyCh) == 0 {
			return nil, errors.New("unready chan is unbuffered")
		}
	}

	for _, readyCh := range c.readyChs {
		if readyCh == nil {
			return nil, errors.New("ready chan must be not nil")
		}

		if cap(readyCh) == 0 {
			return nil, errors.New("ready chan is unbuffered")
		}
	}

	if len(c.amqpUrls) == 0 {
		return nil, fmt.Errorf("url(s) must be set")
	}

	if c.config.ctx != nil {
		c.ctx, c.cancelFunc = context.WithCancel(c.config.ctx)
	} else {
		c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	}

	if c.retryPeriod <= 0 {
		return nil, fmt.Errorf("retryPeriod must be greater then zero")
	}

	go c.connectState()

	return c, nil
}

func WithURL(url string, urls ...string) Option {
	return func(c *Dialer) {
		c.amqpUrls = append(c.amqpUrls, url)
		c.amqpUrls = append(c.amqpUrls, urls...)
	}
}

func WithAMQPDial(dial func(url string, c amqp.Config) (AMQPConnection, error)) Option {
	return func(c *Dialer) {
		c.amqpDial = dial
	}
}

func WithLogger(l logger.Logger) Option {
	return func(c *Dialer) {
		c.logger = l
	}
}

func WithContext(ctx context.Context) Option {
	return func(c *Dialer) {
		c.config.ctx = ctx
	}
}

func WithRetryPeriod(dur time.Duration) Option {
	return func(c *Dialer) {
		c.retryPeriod = dur
	}
}

func WithConnectionProperties(props amqp.Table) Option {
	return func(c *Dialer) {
		c.amqpConfig.Properties = props
	}
}

func WithReadyCh(readyCh chan struct{}) Option {
	return func(c *Dialer) {
		c.readyChs = append(c.readyChs, readyCh)
	}
}

func WithUnreadyCh(unreadyCh chan error) Option {
	return func(c *Dialer) {
		c.unreadyChs = append(c.unreadyChs, unreadyCh)
	}
}

func (c *Dialer) ConnectionCh() <-chan *Connection {
	return c.connCh
}

func (c *Dialer) NotifyReady(readyCh chan struct{}) <-chan struct{} {
	if cap(readyCh) == 0 {
		panic("ready chan is unbuffered")
	}

	select {
	case <-c.NotifyClosed():
		return readyCh
	default:
		c.mu.Lock()
		defer c.mu.Unlock()
		c.readyChs = append(c.readyChs, readyCh)
		return readyCh
	}
}

func (c *Dialer) NotifyUnready(unreadyCh chan error) <-chan error {
	if cap(unreadyCh) == 0 {
		panic("unready chan is unbuffered")
	}

	select {
	case <-c.NotifyClosed():
		close(unreadyCh)
	default:
		c.mu.Lock()
		defer c.mu.Unlock()
		c.unreadyChs = append(c.unreadyChs, unreadyCh)
	}

	return unreadyCh
}

func (c *Dialer) NotifyClosed() <-chan struct{} {
	return c.closedCh
}

func (c *Dialer) Close() {
	c.cancelFunc()
}

func (c *Dialer) Connection(ctx context.Context) (*amqp.Connection, error) {
	select {
	case <-c.ctx.Done():
		return nil, fmt.Errorf("connection closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-c.connCh:
		if !ok {
			return nil, fmt.Errorf("connection closed")
		}

		return conn.AMQPConnection(), nil
	}
}

func (c *Dialer) Consumer(queue string, handler consumer.Handler, opts ...consumer.Option) (*consumer.Consumer, error) {
	opts = append([]consumer.Option{
		consumer.WithLogger(c.logger),
		consumer.WithContext(c.ctx),
	}, opts...)

	return NewConsumer(queue, handler, c.ConnectionCh(), opts...)
}

func (c *Dialer) Publisher(opts ...publisher.Option) (*publisher.Publisher, error) {
	opts = append([]publisher.Option{
		publisher.WithLogger(c.logger),
		publisher.WithContext(c.ctx),
	}, opts...)

	return NewPublisher(c.ConnectionCh(), opts...)
}

func (c *Dialer) connectState() {
	defer close(c.connCh)
	defer close(c.closedCh)
	defer c.cancelFunc()
	defer c.logger.Printf("[DEBUG] connection closed")

	i := 0
	l := len(c.amqpUrls)

	c.logger.Printf("[DEBUG] connection unready")
	var connErr error = amqp.ErrClosed

	c.notifyUnready(connErr)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		i = (i + 1) % l
		url := c.amqpUrls[i]

		connCh := make(chan AMQPConnection)
		errorCh := make(chan error)

		go func() {
			c.logger.Printf("[DEBUG] dialing")
			if conn, err := c.amqpDial(url, c.amqpConfig); err != nil {
				errorCh <- err
			} else {
				connCh <- conn
			}
		}()

	loop2:
		for {
			select {
			case conn := <-connCh:
				select {
				case <-c.ctx.Done():
					c.closeConn(conn)
					return
				default:
				}

				if err := c.connectedState(conn); err != nil {
					c.logger.Printf("[DEBUG] connection unready")
					break loop2
				}

				return
			case err := <-errorCh:
				c.logger.Printf("[DEBUG] connection unready: %v", err)
				if retryErr := c.waitRetry(err); retryErr != nil {
					connErr = retryErr
					break loop2
				}

				return
			}
		}
	}
}

func (c *Dialer) connectedState(amqpConn AMQPConnection) error {
	defer c.closeConn(amqpConn)

	lostCh := make(chan struct{})
	defer close(lostCh)

	internalCloseCh := amqpConn.NotifyClose(make(chan *amqp.Error, 1))

	conn := &Connection{amqpConn: amqpConn, lostCh: lostCh, closeCh: c.closedCh}

	c.logger.Printf("[DEBUG] connection ready")
	c.notifyReady()
	for {
		select {
		case c.connCh <- conn:
			continue
		case err, ok := <-internalCloseCh:
			if !ok {
				err = amqp.ErrClosed
			}

			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Dialer) notifyUnready(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.unreadyChs {
		select {
		case ch <- err:
		default:
		}
	}
}

func (c *Dialer) notifyReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.readyChs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (c *Dialer) waitRetry(err error) error {
	timer := time.NewTimer(c.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()

	c.notifyUnready(err)

	for {
		select {
		case <-timer.C:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Dialer) closeConn(conn AMQPConnection) {
	if err := conn.Close(); err == amqp.ErrClosed {
		return
	} else if err != nil {
		c.logger.Printf("[ERROR] %s", err)
	}
}
