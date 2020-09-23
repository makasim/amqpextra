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
	retryPeriod  time.Duration
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
		retryPeriod:  time.Second * 5,
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
		go c.connectionState()
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

func (c *Consumer) connectionState() {
	defer close(c.doneCh)
	defer c.logger.Printf("[DEBUG] consumer stopped")

	worker := c.chain(c.middlewares, c.worker)

	c.logger.Printf("[DEBUG] consumer starting")
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

			if err := c.channelState(conn, worker); err == nil {
				return
			}

			c.logger.Printf("[DEBUG] consumer unready")
		case c.unreadyCh <- struct{}{}:
		case <-c.closeCh:
			continue
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) channelState(conn *amqp.Connection, worker Worker) error {
	for {
		ch, msgCh, err := c.initFunc(conn)
		if err != nil {
			c.logger.Printf("[ERROR] init func: %s", err)
			return c.waitRetry(err)
		}

		err = c.consumeState(ch, msgCh, worker)
		if err == errChannelClosed {
			continue
		}

		return err
	}
}

func (c *Consumer) consumeState(ch *amqp.Channel, msgCh <-chan amqp.Delivery, worker Worker) error {
	var wg sync.WaitGroup

	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	workerCtx, workerCancelFunc := context.WithCancel(c.ctx)
	defer workerCancelFunc()

	c.runWorkers(workerCtx, msgCh, worker, &wg)
	c.logger.Printf("[DEBUG] workers started")

	for {
		select {
		case c.readyCh <- struct{}{}:
		case <-chCloseCh:
			c.logger.Printf("[DEBUG] workers stopped: channel closed")

			workerCancelFunc()
			wg.Wait()

			return errChannelClosed
		case err := <-c.closeCh:
			c.logger.Printf("[DEBUG] workers stopped: connection closed")

			workerCancelFunc()
			wg.Wait()

			return err
		case <-c.ctx.Done():
			c.logger.Printf("[DEBUG] workers stopped: context closed")

			workerCancelFunc()
			wg.Wait()

			c.close(ch)

			return nil
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
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
}

func (c *Consumer) waitRetry(err error) error {
	timer := time.NewTimer(c.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()

	for {
		select {
		case c.unreadyCh <- struct{}{}:
			continue
		case <-timer.C:
			return err
		case <-c.ctx.Done():
			return nil
		}
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
