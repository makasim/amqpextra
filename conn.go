package amqpextra

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dialFunc     func() (*amqp.Connection, error)
	logErrFunc   func(format string, v ...interface{})
	logDebugFunc func(format string, v ...interface{})

	ctx      context.Context
	cancel	 context.CancelFunc
	getCh chan struct{
		conn *amqp.Connection
		closeCh chan *amqp.Error
	}
	closeCh chan *amqp.Error
	conn    *amqp.Connection
}

func New(
	dialFunc func() (*amqp.Connection, error),
	logErrFunc func(format string, v ...interface{}),
	logDebugFunc func(format string, v ...interface{}),
) *Conn {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Conn{
		dialFunc:     dialFunc,
		logErrFunc:   logErrFunc,
		logDebugFunc: logDebugFunc,

		ctx:          ctx,
		cancel: 	  cancel,
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

func (c *Conn) Close() error {
	c.cancel()

	return <- c.closeCh
}

func (c *Conn) reconnect() {
	for {
		select {
		case <-c.ctx.Done():
			if c.conn == nil {
				return
			}

			if err := c.conn.Close(); err != nil {
				c.logError(err, "connection close errored")
			}

			close(c.getCh)

			return
		default:
		}

		if c.conn == nil || c.conn.IsClosed() {
			conn, err := c.dialFunc()
			if err != nil {
				c.logError(err, "connection dialFunc errored")

				time.Sleep(time.Second * 5)
				c.logDebug("try reconnect")

				continue
			}

			closeCh := make(chan *amqp.Error)
			conn.NotifyClose(closeCh)

			c.conn = conn
			c.closeCh = closeCh

			c.logDebug("connection established")
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
				c.logDebug("connection is closed")

				close(c.getCh)

				return
			}

			c.logError(err, "connection is closed with logError")
		case <-c.ctx.Done():
			if err := c.conn.Close(); err != nil {
				c.logError(err, "connection close errored")
			}

			close(c.getCh)

			return
		}
	}
}

func (c *Conn) logDebug(msg string) {
	if c.logDebugFunc != nil {
		c.logDebugFunc(msg)
	}
}

func (c *Conn) logError(err error, msg string) {
	if c.logErrFunc != nil {
		c.logErrFunc("%s: %s", msg, err)
	}
}
