package amqpextra

import (
	"context"
	"time"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type Connection interface {
	AMQPConnection() *amqp.Connection
	NotifyClose() <-chan *amqp.Error
	Close() error
}

type connection struct {
	amqpConn *amqp.Connection
	closeCh  <-chan *amqp.Error
}

func (c *connection) AMQPConnection() *amqp.Connection {
	return c.amqpConn
}

func (c *connection) NotifyClose() <-chan *amqp.Error {
	return c.closeCh
}

func (c *connection) Close() error {
	return c.amqpConn.Close()
}

type Connector struct {
	dialer Dialer

	logger         logger.Logger
	reconnectSleep time.Duration
	ctx            context.Context
	cancelFunc     context.CancelFunc

	connCh    chan Connection
	readyCh   chan struct{}
	unreadyCh chan error
	closedCh  chan struct{}
}

func New(dialer Dialer) *Connector {
	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Connector{
		dialer: dialer,

		ctx:            ctx,
		cancelFunc:     cancelFunc,
		logger:         logger.Discard,
		reconnectSleep: time.Second * 5,

		connCh:    make(chan Connection),
		readyCh:   make(chan struct{}),
		unreadyCh: make(chan error),
		closedCh:  make(chan struct{}),
	}

	go c.connectState()

	return c
}

func (c *Connector) SetLogger(l logger.Logger) {
	c.logger = l
}

func (c *Connector) SetContext(ctx context.Context) {
	c.ctx, c.cancelFunc = context.WithCancel(ctx)
}

func (c *Connector) SetReconnectSleep(d time.Duration) {
	c.reconnectSleep = d
}

func (c *Connector) Ready() <-chan struct{} {
	return c.readyCh
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

func (c *Connector) ConnCh() (connCh <-chan Connection) {
	select {
	case <-c.ctx.Done():
		connCh := make(chan Connection)
		close(connCh)
		return connCh
	default:
		return c.connCh
	}
}

func (c *Connector) Conn(ctx context.Context) (Connection, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-c.connCh:
		if !ok {
			return nil, amqp.ErrClosed
		}

		return conn, nil
	}
}

func (c *Connector) Consumer(queue string, handler consumer.Handler, opts ...consumer.Option) *consumer.Consumer {
	opts = append([]consumer.Option{
		consumer.WithLogger(c.logger),
		consumer.WithContext(c.ctx),
	}, opts...)

	return NewConsumer(queue, handler, c.ConnCh(), opts...)
}

func (c *Connector) Publisher(opts ...publisher.Option) *publisher.Publisher {
	opts = append([]publisher.Option{
		publisher.WithLogger(c.logger),
		publisher.WithContext(c.ctx),
	}, opts...)

	return NewPublisher(c.ConnCh(), opts...)
}

func (c *Connector) connectState() {
	defer c.close()

	connErr := amqp.ErrClosed
	for {
		select {
		case <-c.ctx.Done():
			return
		case c.unreadyCh <- connErr:
			continue
		default:
		}

		conn, err := c.dialer.Dial()
		if err != nil {
			if err := c.waitRetry(err); err != nil {
				continue
			}

			return
		}

		c.logger.Printf("[DEBUG] connection established")
		if err := c.connectedState(conn); err != nil {
			continue
		}

		return
	}
}

func (c *Connector) connectedState(conn Connection) error {
	defer c.closeConn(conn)

	for {
		select {
		case c.connCh <- conn:
			continue
		case err, ok := <-conn.NotifyClose():
			if !ok {
				err = amqp.ErrClosed
			}

			return err
		case c.readyCh <- struct{}{}:
			continue
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Connector) waitRetry(err error) error {
	timer := time.NewTimer(c.reconnectSleep)
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

func (c *Connector) closeConn(conn Connection) {
	if err := conn.Close(); err == amqp.ErrClosed {
		return
	} else if err != nil {
		// TODO log
	}
}

func (c *Connector) close() {
	c.cancelFunc()
	close(c.unreadyCh)
	close(c.closedCh)
}
