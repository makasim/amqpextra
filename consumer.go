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
	connCh  <-chan *amqp.Connection
	closeCh <-chan *amqp.Error
	ctx     context.Context

	logger      Logger
	middlewares []func(Worker) Worker
}

func NewConsumer(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	ctx context.Context,
	logger Logger,
) *Consumer {
	if logger == nil {
		logger = nilLogger
	}

	return &Consumer{
		connCh:  connCh,
		closeCh: closeCh,
		ctx:     ctx,

		logger: logger,
	}
}

func (c *Consumer) Run(
	num int,
	initFunc func(conn *amqp.Connection) (<-chan amqp.Delivery, error),
	worker Worker,
) {
	var wg sync.WaitGroup
	worker = c.chain(c.middlewares, worker)

L1:
	for {
		select {
		case conn, ok := <-c.connCh:
			if !ok {
				break L1
			}

			select {
			case <-c.closeCh:
				continue L1
			default:
			}

			msgCh, err := initFunc(conn)
			if err != nil {
				c.logger.Printf("[ERROR] init func: %s", err)

				select {
				case <-time.NewTimer(time.Second * 5).C:
					continue
				case <-c.ctx.Done():
					break L1
				}
			}

			c.logger.Printf("[DEBUG] consumer starting")

			msgCloseCh := make(chan struct{})
			workerMsgCh := make(chan amqp.Delivery)

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(msgCloseCh)
				defer close(workerMsgCh)

				for {
					select {
					case msg, ok := <-msgCh:
						if !ok {
							return
						}

						workerMsgCh <- msg
					case <-c.ctx.Done():
						return
					}
				}
			}()

			workerCtx, closeCtx := context.WithCancel(c.ctx)
			for i := 0; i < num; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for {
						select {
						case msg, ok := <-workerMsgCh:
							if !ok {

								return
							}

							if res := worker.ServeMsg(msg, c.ctx); res != nil {
								c.logger.Printf("[ERROR] worker.serveMsg: non nil result: %#v", res)
							}
						case <-workerCtx.Done():
							return
						}
					}
				}()
			}

			c.logger.Printf("[DEBUG] workers started")
			select {
			case <-c.closeCh:
				closeCtx()

				wg.Wait()

				c.logger.Printf("[DEBUG] workers stopped")

				continue L1
			case <-msgCloseCh:
				c.logger.Printf("[DEBUG] msg channel closed")
				closeCtx()

				wg.Wait()

				c.logger.Printf("[DEBUG] workers stopped")

				continue L1
			case <-c.ctx.Done():
				closeCtx()

				wg.Wait()

				c.logger.Printf("[DEBUG] consumer stopped")

				break L1
			}
		case <-c.ctx.Done():
			break L1
		}
	}
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
