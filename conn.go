package amqpextra

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dial     func() (*amqp.Connection, error)
	logErr   func(format string, v ...interface{})
	logDebug func(format string, v ...interface{})
	ctx      context.Context

	getCh chan struct{
		conn *amqp.Connection
		closeCh chan *amqp.Error
	}
	closeCh chan *amqp.Error
	conn    *amqp.Connection
}

func New(
	dial func() (*amqp.Connection, error),
	logErr func(format string, v ...interface{}),
	logDebug func(format string, v ...interface{}),
	ctx context.Context,
) *Conn {
	c := &Conn{
		dial:     dial,
		logErr:   logErr,
		logDebug: logDebug,
		ctx:      ctx,

		getCh: make(chan struct{
			conn *amqp.Connection
			closeCh chan *amqp.Error
		}),
	}

	go c.reconnect()

	return c
}

func (c *Conn) Get() (*amqp.Connection, <-chan *amqp.Error) {
	res, ok := <-c.getCh
	if !ok {
		closeCh := make(chan *amqp.Error)
		close(closeCh)

		return nil, closeCh
	}

	return res.conn, res.closeCh
}

func (c *Conn) reconnect() {
	for {
		select {
		case <-c.ctx.Done():
			if c.conn == nil {
				return
			}

			if err := c.conn.Close(); err != nil {
				c.error(err, "connection close errored")
			}

			close(c.getCh)

			return
		default:
		}

		if c.conn == nil || c.conn.IsClosed() {
			conn, err := c.dial()
			if err != nil {
				c.error(err, "connection dial errored")

				time.Sleep(time.Second * 5)
				c.debug("try reconnect")

				continue
			}

			closeCh := make(chan *amqp.Error)
			conn.NotifyClose(closeCh)

			c.conn = conn
			c.closeCh = closeCh

			c.debug("connection established")
		}

		closeCh := make(chan *amqp.Error)
		c.conn.NotifyClose(closeCh)

		res := struct {
			conn    *amqp.Connection
			closeCh chan *amqp.Error
		}{conn: c.conn, closeCh: closeCh}

		select {
		case c.getCh <- res:
		case err, ok := <-c.closeCh:
			if !ok {
				c.debug("connection is closed")

				close(c.getCh)

				return
			}

			c.error(err, "connection is closed with error")
		case <-c.ctx.Done():
			if err := c.conn.Close(); err != nil {
				c.error(err, "connection close errored")
			}

			close(c.getCh)

			return
		}
	}
}

func (c *Conn) debug(msg string) {
	if c.logDebug != nil {
		c.logDebug(msg)
	}
}

func (c *Conn) error(err error, msg string) {
	if c.logErr != nil {
		c.logErr("%s: %s", msg, err)
	}
}
