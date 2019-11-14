package amqpextra

import (
	"context"
	"github.com/streadway/amqp"
	"sync"
	"time"


)

type Consumer struct {
	connCh  <-chan *amqp.Connection
	closeCh <-chan *amqp.Error
	doneCh  <-chan struct{}
	logErrFunc func(format string, v ...interface{})
	logDbgFunc func(format string, v ...interface{})
}

func NewConsumer(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	doneCh <-chan struct{},
	logErrFunc func(format string, v ...interface{}),
	logDbgFunc func(format string, v ...interface{}),
) *Consumer {
	return &Consumer{
		connCh:  connCh,
		closeCh: closeCh,
		doneCh:  doneCh,
		logErrFunc: logErrFunc,
		logDbgFunc: logDbgFunc,
	}
}

func (c *Consumer) Run(
	num int,
	initFunc func(conn *amqp.Connection) (<-chan amqp.Delivery, error),
	consumeFunc func(msg amqp.Delivery),
) {
	var wg sync.WaitGroup

	c.logDbgFunc("starting consumers")

	L1:
	for {
		select {
		case conn, ok := <-c.connCh:
			if !ok {
				break L1
			}

			msgCh, err := initFunc(conn)
			if err != nil {
				c.logErrFunc("init func: %s", err)
				time.Sleep(time.Second * 5)

				continue
			}

			workerCtx, closeCtx := context.WithCancel(context.Background())
			for i := 0; i < num; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for {
						select {
						case msg := <-msgCh:
							consumeFunc(msg)
						case <-workerCtx.Done():
							return
						}
					}
				}()
			}

			c.logDbgFunc("consumers started")

			select {
			case <-c.closeCh:
				closeCtx()

				wg.Wait()

				c.logDbgFunc("consumers stopped")
			case <-c.doneCh:
				closeCtx()

				break L1
			}
		case <-c.doneCh:
			break L1
		default:
		}
	}

	wg.Wait()

	c.logDbgFunc("consumers stopped")
}


