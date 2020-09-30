package amqpextra

import (
	"context"
	"time"

	"fmt"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type Option func(c *Dialer)

type Connection interface {
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Close() error
}

type Ready struct {
	conn    Connection
	closeCh chan struct{}
}

func (c *Ready) Conn() *amqp.Connection {
	return c.conn.(*amqp.Connection)
}

func (c *Ready) NotifyClose() <-chan struct{} {
	return c.closeCh
}

type config struct {
	amqpUrls   []string
	amqpDial   func(url string, c amqp.Config) (Connection, error)
	amqpConfig amqp.Config

	logger      logger.Logger
	retryPeriod time.Duration
	ctx         context.Context
}

type Dialer struct {
	config

	ctx        context.Context
	cancelFunc context.CancelFunc

	readyCh   chan Ready
	unreadyCh chan error
	closedCh  chan struct{}
}

func Dial(opts ...Option) (*amqp.Connection, error) {
	d, err := New(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()

	return d.Connection(ctx)
}

func New(opts ...Option) (*Dialer, error) {
	c := &Dialer{
		config: config{
			amqpUrls: make([]string, 0, 1),
			amqpDial: func(url string, c amqp.Config) (Connection, error) {
				return amqp.DialConfig(url, c)
			},
			amqpConfig: amqp.Config{
				Heartbeat: time.Second * 30,
				Locale:    "en_US",
			},

			retryPeriod: time.Second * 5,
			logger:      logger.Discard,
		},

		readyCh:   make(chan Ready),
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
		return nil, fmt.Errorf("retryPeriod must be gerater then zero")
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

func WithAMQPDial(dial func(url string, c amqp.Config) (Connection, error)) Option {
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
		c.ctx = ctx
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

func (c *Dialer) NotifyReady() <-chan Ready {
	select {
	case <-c.ctx.Done():
		readyCh := make(chan Ready)
		close(readyCh)
		return readyCh
	default:
		return c.readyCh
	}
}

func (c *Dialer) NotifyUnready() <-chan error {
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
		return nil, c.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case est, ok := <-c.readyCh:
		if !ok {
			return nil, amqp.ErrClosed
		}

		return est.Conn(), nil
	}
}

func (c *Dialer) Consumer(queue string, handler consumer.Handler, opts ...consumer.Option) *consumer.Consumer {
	opts = append([]consumer.Option{
		consumer.WithLogger(c.logger),
		consumer.WithContext(c.ctx),
	}, opts...)

	return NewConsumer(queue, handler, c.NotifyReady(), opts...)
}

func (c *Dialer) Publisher(opts ...publisher.Option) *publisher.Publisher {
	opts = append([]publisher.Option{
		publisher.WithLogger(c.logger),
		publisher.WithContext(c.ctx),
	}, opts...)

	return NewPublisher(c.NotifyReady(), opts...)
}

func (c *Dialer) connectState() {
	defer close(c.closedCh)
	defer close(c.unreadyCh)
	defer c.cancelFunc()

	i := 0
	l := len(c.amqpUrls)

loop0:
	for {
		i = (i + 1) % l
		url := c.amqpUrls[i]

		connCh := make(chan Connection)
		errorCh := make(chan error)

		go func() {
			if conn, err := c.amqpDial(url, c.amqpConfig); err != nil {
				select {
				case errorCh <- err:
				case <-c.ctx.Done():
					return
				}
			} else {
				select {
				case connCh <- conn:
				case <-c.ctx.Done():
					conn.Close()
					return
				}
			}
		}()

		for {
			select {
			case c.unreadyCh <- amqp.ErrClosed:
				continue
			case conn := <-connCh:
				if err := c.connectedState(conn); err != nil {
					break loop0
				}

				return
			case err := <-errorCh:
				c.waitRetry(err)
				break loop0
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func (c *Dialer) connectedState(conn Connection) error {
	defer c.logger.Printf("[DEBUG] connection closed")
	defer c.closeConn(conn)

	closeCh := make(chan struct{})
	defer close(closeCh)

	internalCloseCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	c.logger.Printf("[DEBUG] connection established")
	for {
		select {
		case c.readyCh <- Ready{conn: conn, closeCh: closeCh}:
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

func (c *Dialer) waitRetry(err error) {
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
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Dialer) closeConn(conn Connection) {
	if err := conn.Close(); err == amqp.ErrClosed {
		return
	} else if err != nil {
		c.logger.Printf("[ERROR] %s", err)
	}
}
