package amqpextra

import (
	"time"

	"github.com/streadway/amqp"
)

type Conn struct {
	dialFunc     func() (*amqp.Connection, error)
	logErrFunc   func(format string, v ...interface{})
	logDebugFunc func(format string, v ...interface{})

	closeChCh chan chan *amqp.Error
	connCh    chan *amqp.Connection
	doneCh    chan struct{}
}

func New(
	dialFunc func() (*amqp.Connection, error),
	logErrFunc func(format string, v ...interface{}),
	logDebugFunc func(format string, v ...interface{}),
) *Conn {
	c := &Conn{
		dialFunc:     dialFunc,
		logErrFunc:   logErrFunc,
		logDebugFunc: logDebugFunc,

		doneCh:    make(chan struct{}),
		closeChCh: make(chan chan *amqp.Error),
		connCh:    make(chan *amqp.Connection),
	}

	go c.reconnect()

	return c
}

func (c *Conn) Get() (<-chan *amqp.Connection, <-chan *amqp.Error) {
	return c.connCh, <-c.closeChCh
}

func (c *Conn) Close() error {
	closeCh, ok := <-c.closeChCh
	if !ok {
		return nil
	}

	close(c.doneCh)

	return <-closeCh
}

func (c *Conn) reconnect() {
	L1:
	for {
		select {
		case <-c.doneCh:
			close(c.connCh)
			close(c.closeChCh)

			return
		default:
		}

		conn, err := c.dialFunc()
		if err != nil {
			c.logError(err)

			time.Sleep(time.Second * 5)
			c.logDebug("try reconnect")

			continue
		}

		closeCh := make(chan *amqp.Error)
		conn.NotifyClose(closeCh)

		c.logDebug("connection established")

		nextCloseCh := make(chan *amqp.Error)
		conn.NotifyClose(nextCloseCh)

		for {
			select {
			case c.closeChCh <- nextCloseCh:
				nextCloseCh = make(chan *amqp.Error)
				conn.NotifyClose(nextCloseCh)
			case c.connCh <- conn:
			case <-closeCh:
				continue L1
			case <-c.doneCh:
				if err := conn.Close(); err != nil {
					c.logError(err)
				}

				close(c.closeChCh)
				close(c.connCh)

				return
			}
		}
	}
}

func (c *Conn) logDebug(msg string) {
	c.logDebugFunc(msg)
}

func (c *Conn) logError(err error) {
	c.logErrFunc(err.Error())
}
