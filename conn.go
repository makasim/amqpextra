package amqpextra

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
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
	inited          bool
}

func New(dialer Dialer) *Conn {
	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Conn{
		dialer:         dialer,
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		logger:         nilLogger,
		reconnectSleep: time.Second * 5,

		inited:    false,
		closeChCh: make(chan chan *amqp.Error),
		connCh:    make(chan *amqp.Connection),
	}

	return c
}

func (c *Conn) SetLogger(logger Logger) {
	if !c.inited {
		c.logger = logger
	}
}

func (c *Conn) SetContext(ctx context.Context) {
	if !c.inited {
		c.ctx, c.cancelFunc = context.WithCancel(ctx)
	}
}

func (c *Conn) SetReconnectSleep(d time.Duration) {
	if !c.inited {
		c.reconnectSleep = d
	}
}

func (c *Conn) Start() {
	c.once.Do(func() {
		c.inited = true
		go c.reconnect()
	})
}

func (c *Conn) Stop() {
	c.cancelFunc()
}

func (c *Conn) Get() (<-chan *amqp.Connection, <-chan *amqp.Error) {
	c.Start()

	return c.connCh, <-c.closeChCh
}

func (c *Conn) Consumer(l Logger) *Consumer {
	connCh, closeCh := c.Get()

	if l == nil {
		l = c.logger
	}

	return NewConsumer(connCh, closeCh, c.ctx, l)
}

func (c *Conn) Publisher(initFunc func(conn *amqp.Connection) (*amqp.Channel, error), l Logger) *Publisher {
	connCh, closeCh := c.Get()

	if l == nil {
		l = c.logger
	}

	return NewPublisher(connCh, closeCh, c.ctx, initFunc, l)
}

func (c *Conn) reconnect() {
L1:
	for {
		select {
		case <-c.ctx.Done():
			c.close()

			break L1
		default:
		}

		conn, err := c.dialer()
		if err != nil {
			c.logger.Printf("[ERROR] %s", err)

			select {
			case <-time.NewTimer(c.reconnectSleep).C:
				c.logger.Printf("[DEBUG] try reconnect")

				continue L1
			case <-c.ctx.Done():
				c.close()

				break L1
			}
		}

		c.internalCloseCh = make(chan *amqp.Error)
		conn.NotifyClose(c.internalCloseCh)

		c.logger.Printf("[DEBUG] connection established")

		nextCloseCh := make(chan *amqp.Error, 1)

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
					case <-time.NewTimer(time.Second * 5).C:
						c.logger.Printf("[WARN] closeCh has not been read out within safeguard time. Make sure you are reading closeCh.")
					}
				}

				continue L1
			case <-c.ctx.Done():
				c.close()

				if err := conn.Close(); err != nil {
					c.logger.Printf("[ERROR] %s", err)
				}

				c.logger.Printf("[DEBUG] connection is closed")

				for _, closeCh := range c.closeChs {
					close(closeCh)
				}

				break L1
			}
		}
	}
}

func (c *Conn) close() {
	close(c.connCh)
	close(c.closeChCh)
}
