package amqpextra

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dialFunc func() (*amqp.Connection, error)
	ctx      context.Context

	closeChCh       chan chan *amqp.Error
	connCh          chan *amqp.Connection
	closeChs        []chan *amqp.Error
	internalCloseCh chan *amqp.Error
	logger          *logger
}

func New(
	dialFunc func() (*amqp.Connection, error),
	ctx context.Context,
) *Conn {
	c := &Conn{
		dialFunc: dialFunc,
		ctx:      ctx,

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
	consumer.logger.debugFunc = c.logger.debugFunc
	consumer.logger.errorFunc = c.logger.errorFunc

	return consumer
}

func (c *Conn) Publisher(initFunc func(conn *amqp.Connection) (*amqp.Channel, error)) *Publisher {
	connCh, closeCh := c.Get()

	publisher := NewPublisher(connCh, closeCh, c.ctx, initFunc)
	publisher.logger.debugFunc = c.logger.debugFunc
	publisher.logger.errorFunc = c.logger.errorFunc

	return publisher
}

func (c *Conn) reconnect() {
L1:
	for {
		select {
		case <-c.ctx.Done():
			close(c.connCh)
			close(c.closeChCh)

			return
		default:
		}

		conn, err := c.dialFunc()
		if err != nil {
			c.logger.Errorf("%s", err)

			time.Sleep(time.Second * 5)
			c.logger.Debugf("try reconnect")

			continue
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
				close(c.closeChCh)
				close(c.connCh)

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
