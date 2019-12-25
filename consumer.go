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
	queue   string
	worker  Worker
	connCh  <-chan *amqp.Connection
	closeCh <-chan *amqp.Error

	once         sync.Once
	started      bool
	workerNum    int
	restartSleep time.Duration
	initFunc     func(conn *amqp.Connection) (<-chan amqp.Delivery, error)
	ctx          context.Context
	cancelFunc   context.CancelFunc
	logger       Logger
	middlewares  []func(Worker) Worker
	doneCh       chan struct{}
}

func NewConsumer(
	queue string,
	worker Worker,
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
) *Consumer {
	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Consumer{
		queue:   queue,
		worker:  worker,
		connCh:  connCh,
		closeCh: closeCh,

		started:      false,
		workerNum:    1,
		restartSleep: time.Second * 5,
		ctx:          ctx,
		cancelFunc:   cancelFunc,
		logger:       nilLogger,
		doneCh:       make(chan struct{}),
		initFunc: func(conn *amqp.Connection) (<-chan amqp.Delivery, error) {
			ch, err := conn.Channel()
			if err != nil {
				return nil, err
			}

			return ch.Consume(
				queue,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
		},
	}
}

func (c *Consumer) SetLogger(logger Logger) {
	if !c.started {
		c.logger = logger
	}
}

func (c *Consumer) SetContext(ctx context.Context) {
	if !c.started {
		c.ctx, c.cancelFunc = context.WithCancel(ctx)
	}
}

func (c *Consumer) SetRestartSleep(d time.Duration) {
	if !c.started {
		c.restartSleep = d
	}
}

func (c *Consumer) SetWorkerNum(n int) {
	if !c.started {
		c.workerNum = n
	}
}

func (c *Consumer) SetInitFunc(f func(conn *amqp.Connection) (<-chan amqp.Delivery, error)) {
	if !c.started {
		c.initFunc = f
	}
}

func (c *Consumer) Use(middlewares ...func(Worker) Worker) {
	c.middlewares = append(c.middlewares, middlewares...)
}

func (c *Consumer) Start() {
	c.once.Do(func() {
		c.started = true
		go c.start()
	})
}

func (c *Consumer) Run() {
	c.Start()

	<-c.doneCh
}

func (c *Consumer) Stop() {
	c.cancelFunc()
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

func (c *Consumer) start() {
	defer close(c.doneCh)

	var wg sync.WaitGroup
	worker := c.chain(c.middlewares, c.worker)

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

			msgCh, err := c.initFunc(conn)
			if err != nil {
				c.logger.Printf("[ERROR] init func: %s", err)

				select {
				case <-time.NewTimer(c.restartSleep).C:
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
			for i := 0; i < c.workerNum; i++ {
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
