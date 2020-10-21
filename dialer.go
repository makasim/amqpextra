package amqpextra

import (
	"context"
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

	connCh    chan *Connection
	readyCh   chan struct{}

	mu *sync.Mutex
	notifyReady []chan struct{}
	notifyUnready []chan struct{}

	unreadyCh chan error
	closedCh  chan struct{}
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

		connCh:    make(chan *Connection),
		readyCh:   make(chan struct{}),
		unreadyCh: make(chan error),
		closedCh:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
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

func WithReadyChan(readyCh chan struct{}) Option {
	return func(c *Dialer) {
		if readyCh == nil {
			readyCh = make(chan struct{}, 1)
		}
		c.readyCh = readyCh
	}
}

func WithUnreadyChan(unreadyCh chan error) Option {
	f := func(c *Dialer) {
		if unreadyCh == nil {
			unreadyCh = make(chan error, 1)
		}
		c.unreadyCh = unreadyCh
	}
	return f
}

func (c *Dialer) ConnectionCh() <-chan *Connection {
	return c.connCh
}

func (c *Dialer) NotifyReady(receiver chan struct{}) <-chan struct{} {
	if cap(receiver) == 0 {
		panic("receiver chan must be buffered")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <- c.NotifyClosed():
		close(receiver)
	default:
	}

	return c.readyCh
}

func (c *Dialer) NotifyUnready(receiver chan struct{}) <-chan error {
	if cap(receiver) == 0 {
		panic("unreadyCh must be buffered")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <- c.NotifyClosed():
		close(receiver)
	default:
		c.notifyUnready = append(c.notifyUnready, receiver)
	}

	return c.unreadyCh
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
	defer close(c.unreadyCh)
	defer c.cancelFunc()
	defer c.logger.Printf("[DEBUG] connection closed")

	i := 0
	l := len(c.amqpUrls)

	c.logger.Printf("[DEBUG] connection unready")
	var connErr error = amqp.ErrClosed
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
			case c.unreadyCh <- connErr:
				continue
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
	for {
		select {
		case c.readyCh <- struct{}{}:
			continue
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

func (c *Dialer) waitRetry(err error) error {
	timer := time.NewTimer(c.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()

	for {
		select {
		case c.unreadyCh <- err:
			continue
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
