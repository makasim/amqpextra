package amqpextra

import (
	"context"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Worker interface {
	ServeMsg(msg amqp.Delivery, ctx context.Context) interface{}
}

type WorkerFunc func(msg amqp.Delivery, ctx context.Context) interface{}

func (f WorkerFunc) ServeMsg(msg amqp.Delivery, ctx context.Context) interface{} {
	return f(msg, ctx)
}

type Consumer struct {
	connCh     <-chan *amqp.Connection
	closeCh    <-chan *amqp.Error
	ctx        context.Context
	logErrFunc func(format string, v ...interface{})
	logDbgFunc func(format string, v ...interface{})

	middlewares []func(Worker) Worker
}

func NewConsumer(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	ctx context.Context,
	logErrFunc func(format string, v ...interface{}),
	logDbgFunc func(format string, v ...interface{}),
) *Consumer {
	return &Consumer{
		connCh:     connCh,
		closeCh:    closeCh,
		ctx:        ctx,
		logErrFunc: logErrFunc,
		logDbgFunc: logDbgFunc,
	}
}

func (c *Consumer) Run(
	num int,
	initFunc func(conn *amqp.Connection) (<-chan amqp.Delivery, error),
	worker Worker,
) {
	var wg sync.WaitGroup
	c.logDbgFunc("consumer starting")

	worker = c.chain(c.middlewares, worker)

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

			workerCtx, closeCtx := context.WithCancel(c.ctx)
			for i := 0; i < num; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for {
						select {
						case msg := <-msgCh:
							if res := worker.ServeMsg(msg, c.ctx); res != nil {
								c.logErrFunc("serveMsg: non nil result: %#v", res)
							}
						case <-workerCtx.Done():
							return
						}
					}
				}()
			}

			c.logDbgFunc("workers started")

			select {
			case <-c.closeCh:
				closeCtx()

				wg.Wait()

				c.logDbgFunc("workers stopped")
			case <-c.ctx.Done():
				closeCtx()

				break L1
			}
		case <-c.ctx.Done():
			break L1
		}
	}

	wg.Wait()

	c.logDbgFunc("consumer stopped")
}

func (c *Consumer) Use(middlewares ...func(Worker) Worker) {
	c.middlewares = append(c.middlewares, middlewares...)
}

func (*Consumer) chain(middlewares []func(Worker) Worker, endpoint Worker) Worker {
	// Return ahead of time if there aren't any middlewares for the chain
	if len(middlewares) == 0 {
		return endpoint
	}

	// Wrap the end handler with the middleware chain
	w := middlewares[len(middlewares)-1](endpoint)
	for i := len(middlewares) - 2; i >= 0; i-- {
		w = middlewares[i](w)
	}

	return w
}
