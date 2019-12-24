package amqpextra

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dialer Dialer
	ctx    context.Context

	once            sync.Once
	closeChCh       chan chan *amqp.Error
	connCh          chan *amqp.Connection
	closeChs        []chan *amqp.Error
	internalCloseCh chan *amqp.Error
	logger          Logger
}

func New(dialer Dialer, ctx context.Context, logger Logger) *Conn {
	if logger == nil {
		logger = nilLogger()
	}

	c := &Conn{
		dialer: dialer,
		ctx:    ctx,
		logger: logger,

		closeChCh: make(chan chan *amqp.Error),
		connCh:    make(chan *amqp.Connection),
	}

	return c
}

func (c *Conn) Get() (<-chan *amqp.Connection, <-chan *amqp.Error) {
	c.once.Do(func() {
		go c.reconnect()
	})

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
			case <-time.NewTimer(time.Second * 5).C:
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
						c.logger.Printf("[WARN] closeCh has not been read within safeguard time. Make sure you are reading closeCh out.")
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
