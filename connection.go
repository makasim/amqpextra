package amqpextra

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Connection struct {
	dialer Dialer

	once            sync.Once
	closeChCh       chan chan *amqp.Error
	connCh          chan *amqp.Connection
	closeChs        []chan *amqp.Error
	internalCloseCh chan *amqp.Error
	logger          Logger
	reconnectSleep  time.Duration
	ctx             context.Context
	cancelFunc      context.CancelFunc
	started         bool
	readyCh         chan struct{}
	unreadyCh       chan struct{}
}

func New(dialer Dialer) *Connection {
	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Connection{
		dialer:         dialer,
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		logger:         nilLogger,
		reconnectSleep: time.Second * 5,

		started:   false,
		closeChCh: make(chan chan *amqp.Error),
		connCh:    make(chan *amqp.Connection),
		readyCh:   make(chan struct{}),
		unreadyCh: make(chan struct{}),
	}

	return c
}

func (c *Connection) SetLogger(logger Logger) {
	if !c.started {
		c.logger = logger
	}
}

func (c *Connection) SetContext(ctx context.Context) {
	if !c.started {
		c.ctx, c.cancelFunc = context.WithCancel(ctx)
	}
}

func (c *Connection) SetReconnectSleep(d time.Duration) {
	if !c.started {
		c.reconnectSleep = d
	}
}

func (c *Connection) Ready() <-chan struct{} {
	c.Start()

	return c.readyCh
}

func (c *Connection) Unready() <-chan struct{} {
	c.Start()

	return c.unreadyCh
}

func (c *Connection) Start() {
	c.once.Do(func() {
		c.started = true
		go c.reconnect()
	})
}

func (c *Connection) Close() {
	c.cancelFunc()
}

// @deprecated
func (c *Connection) Get() (connCh <-chan *amqp.Connection, closeCh <-chan *amqp.Error) {
	return c.ConnCh()
}

// You must use closeCh properly.
func (c *Connection) ConnCh() (connCh <-chan *amqp.Connection, closeCh <-chan *amqp.Error) {
	c.Start()

	return c.connCh, <-c.closeChCh
}

func (c *Connection) Conn() (*amqp.Connection, error) {
	c.Start()

	select {
	case conn, ok := <-c.connCh:
		if !ok {
			return nil, amqp.ErrClosed
		}

		return conn, nil
	case <-c.unreadyCh:
		return nil, amqp.ErrClosed
	}
}

func (c *Connection) Consumer(queue string, worker Worker) *Consumer {
	connCh, closeCh := c.ConnCh()

	consumer := NewConsumer(queue, worker, connCh, closeCh)
	consumer.SetLogger(c.logger)
	consumer.SetContext(c.ctx)

	return consumer
}

func (c *Connection) Publisher() *Publisher {
	connCh, closeCh := c.ConnCh()

	publisher := NewPublisher(connCh, closeCh)
	publisher.SetLogger(c.logger)
	publisher.SetContext(c.ctx)

	return publisher
}

func (c *Connection) reconnect() {
	nextCloseCh := make(chan *amqp.Error, 1)

	for {
		select {
		case c.closeChCh <- nextCloseCh:
			c.closeChs = append(c.closeChs, nextCloseCh)
			nextCloseCh = make(chan *amqp.Error, 1)
		case <-c.ctx.Done():
			c.close(nil)

			return
		case c.unreadyCh <- struct{}{}:
			continue
		default:
		}

		conn, err := c.dialer.Dial()
		if err != nil {
			c.logger.Printf("[ERROR] %s", err)

			timer := time.NewTimer(c.reconnectSleep)

			select {
			case <-timer.C:
				c.logger.Printf("[DEBUG] try reconnect")

				continue
			case <-c.ctx.Done():
				timer.Stop()
				c.close(nil)

				return
			}
		}

		c.logger.Printf("[DEBUG] connection established")

		if !c.serve(conn) {
			return
		}
	}
}

func (c *Connection) serve(conn *amqp.Connection) bool {
	nextCloseCh := make(chan *amqp.Error, 1)

	c.internalCloseCh = make(chan *amqp.Error)
	conn.NotifyClose(c.internalCloseCh)

	for {
		select {
		case c.closeChCh <- nextCloseCh:
			c.closeChs = append(c.closeChs, nextCloseCh)
			nextCloseCh = make(chan *amqp.Error, 1)
		case c.connCh <- conn:
		case err := <-c.internalCloseCh:
			if err == nil {
				err = amqp.ErrClosed
			}

			for _, closeCh := range c.closeChs {
				select {
				case closeCh <- err:
				case <-c.ctx.Done():
					c.close(conn)

					return false
				default:
					// the close channel already contains the error but receiver has not read it out.
					// we can safely skip this time.
				}
			}

			return true
		case c.readyCh <- struct{}{}:
			continue
		case <-c.ctx.Done():
			c.close(conn)

			return false
		}
	}
}

func (c *Connection) close(conn *amqp.Connection) {
	close(c.closeChCh)
	close(c.unreadyCh)

	if conn != nil && !conn.IsClosed() {
		connCloseCh := make(chan *amqp.Error)
		conn.NotifyClose(connCloseCh)

		if err := conn.Close(); err != nil {
			c.logger.Printf("[ERROR] connection close: %s", err)
		}

		<-connCloseCh
		c.logger.Printf("[DEBUG] connection is closed")
	}

	close(c.connCh)

	for _, closeCh := range c.closeChs {
		close(closeCh)
	}
}
