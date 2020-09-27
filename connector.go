package amqpextra

import (
	"context"
	"time"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type Option func(c *Connector)

type Connection interface {
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Close() error
}

type Established struct {
	conn    Connection
	closeCh chan struct{}
}

func (c *Established) Conn() *amqp.Connection {
	return c.conn.(*amqp.Connection)
}

func (c *Established) NotifyClose() <-chan struct{} {
	return c.closeCh
}

type Connector struct {
	dialer Dialer

	logger      logger.Logger
	retryPeriod time.Duration
	ctx         context.Context
	cancelFunc  context.CancelFunc

	readyCh   chan Established
	unreadyCh chan error
	closedCh  chan struct{}
}

func New(dialer Dialer, opts ...Option) (*Connector, error) {
	c := &Connector{
		dialer: dialer,

		readyCh:   make(chan Established),
		unreadyCh: make(chan error),
		closedCh:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.ctx != nil {
		c.ctx, c.cancelFunc = context.WithCancel(c.ctx)
	} else {
		c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	}

	if c.retryPeriod == 0 {
		c.retryPeriod = time.Second * 5
	}

	if c.logger == nil {
		c.logger = logger.Discard
	}

	go c.connectState()

	return c, nil
}

func WithLogger(l logger.Logger) Option {
	return func(c *Connector) {
		c.logger = l
	}
}

func WithContext(ctx context.Context) Option {
	return func(c *Connector) {
		c.ctx = ctx
	}
}

func WithRetryPeriod(dur time.Duration) Option {
	return func(c *Connector) {
		c.retryPeriod = dur
	}
}

func (c *Connector) Ready() <-chan Established {
	select {
	case <-c.ctx.Done():
		estCh := make(chan Established)
		close(estCh)
		return estCh
	default:
		return c.readyCh
	}
}

func (c *Connector) Unready() <-chan error {
	return c.unreadyCh
}

func (c *Connector) Closed() <-chan struct{} {
	return c.closedCh
}

func (c *Connector) Close() {
	c.cancelFunc()
}

func (c *Connector) Connection(ctx context.Context) (*amqp.Connection, error) {
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

func (c *Connector) Consumer(queue string, handler consumer.Handler, opts ...consumer.Option) *consumer.Consumer {
	opts = append([]consumer.Option{
		consumer.WithLogger(c.logger),
		consumer.WithContext(c.ctx),
	}, opts...)

	return NewConsumer(queue, handler, c.Ready(), opts...)
}

func (c *Connector) Publisher(opts ...publisher.Option) *publisher.Publisher {
	opts = append([]publisher.Option{
		publisher.WithLogger(c.logger),
		publisher.WithContext(c.ctx),
	}, opts...)

	return NewPublisher(c.Ready(), opts...)
}

func (c *Connector) connectState() {
	defer close(c.closedCh)
	defer close(c.unreadyCh)
	defer c.cancelFunc()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		conn, err := c.dialer.Dial()
		if err != nil {
			c.waitRetry(err)
			continue
		}

		if err := c.connectedState(conn); err != nil {
			continue
		}

		return
	}
}

func (c *Connector) connectedState(conn Connection) error {
	defer c.logger.Printf("[DEBUG] connection closed")
	defer c.closeConn(conn)

	closeCh := make(chan struct{})
	defer close(closeCh)

	internalCloseCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	c.logger.Printf("[DEBUG] connection established")
	for {
		select {
		case c.readyCh <- Established{conn: conn, closeCh: closeCh}:
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

func (c *Connector) waitRetry(err error) {
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

func (c *Connector) closeConn(conn Connection) {
	if err := conn.Close(); err == amqp.ErrClosed {
		return
	} else if err != nil {
		c.logger.Printf("[ERROR] %s", err)
	}
}
