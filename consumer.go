package amqpextra

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Worker interface {
	ServeMsg(ctx context.Context, msg amqp.Delivery) interface{}
}

type WorkerFunc func(ctx context.Context, msg amqp.Delivery) interface{}

func (f WorkerFunc) ServeMsg(ctx context.Context, msg amqp.Delivery) interface{} {
	return f(ctx, msg)
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
	initFunc     func(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error)
	ctx          context.Context
	cancelFunc   context.CancelFunc
	logger       Logger
	middlewares  []func(Worker) Worker
	doneCh       chan struct{}
	readyCh      chan struct{}
	unreadyCh    chan struct{}
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
		readyCh:      make(chan struct{}),
		unreadyCh:    make(chan struct{}),
		initFunc: func(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
			ch, err := conn.Channel()
			if err != nil {
				return nil, nil, err
			}

			msgCh, err := ch.Consume(
				queue,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return nil, nil, err
			}

			return ch, msgCh, nil
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

func (c *Consumer) SetInitFunc(f func(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error)) {
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
		go c.connect()
	})
}

func (c *Consumer) Run() {
	c.Start()

	<-c.doneCh
}

func (c *Consumer) Ready() <-chan struct{} {
	return c.readyCh
}

func (c *Consumer) Unready() <-chan struct{} {
	return c.unreadyCh
}

func (c *Consumer) Close() {
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

func (c *Consumer) connect() {
	defer close(c.doneCh)
	defer c.logger.Printf("[DEBUG] consumer stopped")

	worker := c.chain(c.middlewares, c.worker)

	for {
		select {
		case conn, ok := <-c.connCh:
			if !ok {
				return
			}

			select {
			case <-c.closeCh:
				continue
			default:
			}

			ch, msgCh, err := c.initFunc(conn)
			if err != nil {
				c.logger.Printf("[ERROR] init func: %s", err)

				timer := time.NewTimer(c.restartSleep)

				select {
				case <-timer.C:
					continue
				case <-c.ctx.Done():
					timer.Stop()
					return
				}
			}

			c.logger.Printf("[DEBUG] consumer starting")
			if !c.serve(ch, msgCh, worker) {
				return
			}

			if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
				c.logger.Printf("[WARN] consumer: channel close: %s", err)
			}
		case c.unreadyCh <- struct{}{}:
		case <-c.closeCh:
			continue
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) serve(ch *amqp.Channel, msgCh <-chan amqp.Delivery, worker Worker) bool {
	var wg sync.WaitGroup

	workerDoneCh := make(chan struct{})
	workerCtx, workerCancelFunc := context.WithCancel(c.ctx)
	defer workerCancelFunc()

	c.runWorkers(workerCtx, msgCh, worker, &wg)
	c.logger.Printf("[DEBUG] workers started")

	go func() {
		wg.Wait()
		close(workerDoneCh)
	}()

	for {
		select {
		case c.readyCh <- struct{}{}:
		case <-c.closeCh:
			workerCancelFunc()

			wg.Wait()

			c.logger.Printf("[DEBUG] workers stopped")

			return true
		case <-workerDoneCh:
			workerCancelFunc()

			wg.Wait()

			c.logger.Printf("[DEBUG] workers stopped")

			return true
		case <-c.ctx.Done():
			workerCancelFunc()

			wg.Wait()

			c.logger.Printf("[DEBUG] workers stopped")

			c.close(ch)

			return false
		}
	}
}

func (c *Consumer) runWorkers(
	workerCtx context.Context,
	msgCh <-chan amqp.Delivery,
	worker Worker,
	wg *sync.WaitGroup,
) {
	for i := 0; i < c.workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case msg, ok := <-msgCh:
					if !ok {
						return
					}

					if res := worker.ServeMsg(c.ctx, msg); res != nil {
						c.logger.Printf("[ERROR] worker.serveMsg: non nil result: %#v", res)
					}
				case <-c.ctx.Done():
					return
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
}

func (c *Consumer) close(ch *amqp.Channel) {
	if ch == nil {
		return
	}

	if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
		c.logger.Printf("[WARN] channel close: %s", err)
	}

	close(c.unreadyCh)
}
