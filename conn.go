package amqpextra

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dialer Dialer
	ctx    context.Context

	closeChCh       chan chan *amqp.Error
	connCh          chan *amqp.Connection
	closeChs        []chan *amqp.Error
	internalCloseCh chan *amqp.Error
	logger          *logger
}

func New(dialer Dialer, ctx context.Context) *Conn {
	c := &Conn{
		dialer: dialer,
		ctx:    ctx,

		closeChCh: make(chan chan *amqp.Error),
		connCh:    make(chan *amqp.Connection),
		logger:    &logger{},
	}

	go c.reconnect()

	return c
}

func (c *Conn) Get() (<-chan *amqp.Connection, <-chan *amqp.Error) {
	return c.connCh, <-c.closeChCh
}

func (c *Conn) SetDebugFunc(f func(format string, v ...interface{})) {
	c.logger.SetDebugFunc(f)
}

func (c *Conn) SetErrorFunc(f func(format string, v ...interface{})) {
	c.logger.SetErrorFunc(f)
}

func (c *Conn) Consumer() *Consumer {
	connCh, closeCh := c.Get()

	consumer := NewConsumer(connCh, closeCh, c.ctx)
	consumer.logger.SetDebugFunc(c.logger.DebugFunc())
	consumer.logger.SetErrorFunc(c.logger.ErrorFunc())

	return consumer
}

func (c *Conn) Publisher(initFunc func(conn *amqp.Connection) (*amqp.Channel, error)) *Publisher {
	connCh, closeCh := c.Get()

	publisher := NewPublisher(connCh, closeCh, c.ctx, initFunc)
	publisher.logger.SetDebugFunc(c.logger.DebugFunc())
	publisher.logger.SetErrorFunc(c.logger.ErrorFunc())

	return publisher
}

func (c *Conn) reconnect() {
L1:
	for {
		select {
		case <-c.ctx.Done():
			c.close()

			return
		default:
		}

		conn, err := c.dialer()
		if err != nil {
			c.logger.Errorf("%s", err)

			select {
			case <-time.NewTimer(time.Second * 5).C:
				c.logger.Debugf("try reconnect")

				continue
			case <-c.ctx.Done():
				c.close()

				return
			}
		}

		c.internalCloseCh = make(chan *amqp.Error)
		conn.NotifyClose(c.internalCloseCh)

		c.logger.Debugf("connection established")

		nextCloseCh := make(chan *amqp.Error)

		for {
			select {
			case c.closeChCh <- nextCloseCh:
				c.closeChs = append(c.closeChs, nextCloseCh)
				nextCloseCh = make(chan *amqp.Error)
			case c.connCh <- conn:
			case err := <-c.internalCloseCh:
				if err == nil {
					err = amqp.ErrClosed
				}

				for _, closeCh := range c.closeChs {
					closeCh <- err
				}

				continue L1
			case <-c.ctx.Done():
				c.close()

				if err := conn.Close(); err != nil {
					c.logger.Errorf("%s", err)
				}

				c.logger.Debugf("connection is closed")

				for _, closeCh := range c.closeChs {
					close(closeCh)
				}

				return
			}
		}
	}
}

func (c *Conn) close() {
	close(c.connCh)
	close(c.closeChCh)
}
