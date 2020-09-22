package consumer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

var errChannelClosed = fmt.Errorf("channel closed")

type Option func(c *Consumer)

type Consumer struct {
	handler     Handler
	connCh      <-chan Connection
	connCloseCh <-chan *amqp.Error

	worker Worker

	retryPeriod time.Duration
	initFunc    func(conn Connection) (Channel, error)
	ctx         context.Context
	cancelFunc  context.CancelFunc
	logger      logger.Logger
	closeCh     chan struct{}
	readyCh     chan struct{}
	unreadyCh   chan struct{}

	queue     string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

func New(
	queue string,
	handler Handler,
	connCh <-chan Connection,
	closeCh <-chan *amqp.Error,
	opts ...Option,
) *Consumer {
	c := &Consumer{
		queue:       queue,
		handler:     handler,
		connCh:      connCh,
		connCloseCh: closeCh,

		closeCh:   make(chan struct{}),
		readyCh:   make(chan struct{}),
		unreadyCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.ctx != nil {
		c.ctx, c.cancelFunc = context.WithCancel(c.ctx)
	} else {
		c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	}

	if c.retryPeriod == 0 {
		c.retryPeriod = time.Second * 5
	}

	if c.logger == nil {
		c.logger = logger.Discard
	}

	if c.initFunc == nil {
		c.initFunc = func(conn Connection) (Channel, error) {
			ch, err := conn.Channel()
			if err != nil {
				return nil, err
			}

			return ch, nil
		}
	}

	if c.worker == nil {
		c.worker = &defaultWorker{logger: c.logger}
	}

	return c
}

func WithLogger(l logger.Logger) Option {
	return func(c *Consumer) {
		c.logger = l
	}
}

func WithContext(ctx context.Context) Option {
	return func(c *Consumer) {
		c.ctx = ctx
	}
}

func WithRetryPeriod(dur time.Duration) Option {
	return func(c *Consumer) {
		c.retryPeriod = dur
	}
}

func WithInitFunc(f func(conn Connection) (Channel, error)) Option {
	return func(c *Consumer) {
		c.initFunc = f
	}
}

func WithWorker(w Worker) Option {
	return func(c *Consumer) {
		c.worker = w
	}
}

func WithConsumeArgs(consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) Option {
	return func(c *Consumer) {
		c.consumer = consumer
		c.autoAck = autoAck
		c.exclusive = exclusive
		c.noLocal = noLocal
		c.noWait = noWait
		c.args = args
	}
}

func (c *Consumer) Run() {
	c.connectionState()
}

func (c *Consumer) Ready() <-chan struct{} {
	return c.readyCh
}

func (c *Consumer) Unready() <-chan struct{} {
	return c.unreadyCh
}

func (c *Consumer) Closed() <-chan struct{} {
	return c.closeCh
}

func (c *Consumer) Close() {
	c.cancelFunc()
}

func (c *Consumer) connectionState() {
	defer c.cancelFunc()
	defer close(c.unreadyCh)
	defer close(c.closeCh)
	defer c.logger.Printf("[DEBUG] consumer stopped")

	c.logger.Printf("[DEBUG] consumer starting")
	for {
		select {
		case conn, ok := <-c.connCh:
			if !ok {
				return
			}

			select {
			case <-c.connCloseCh:
				continue
			case <-c.ctx.Done():
				return
			default:
			}

			if err := c.channelState(conn); err == nil {
				c.logger.Printf("[DEBUG] consumer unready")
				return
			}

			c.logger.Printf("[DEBUG] consumer unready")
		case c.unreadyCh <- struct{}{}:
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) channelState(conn Connection) error {
	for {
		ch, err := c.initFunc(conn)
		if err != nil {
			c.logger.Printf("[ERROR] init func: %s", err)
			return c.waitRetry(err)
		}

		err = c.consumeState(ch)
		if err == errChannelClosed {
			continue
		}

		return err
	}
}

func (c *Consumer) consumeState(ch Channel) error {
	msgCh, err := ch.Consume(
		c.queue,
		c.consumer,
		c.autoAck,
		c.exclusive,
		c.noLocal,
		c.noWait,
		c.args,
	)
	if err != nil {
		c.logger.Printf("[ERROR] ch.Consume: %s", err)
		return c.waitRetry(err)
	}

	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	workerDoneCh := make(chan struct{})
	workerCtx, workerCancelFunc := context.WithCancel(c.ctx)
	defer workerCancelFunc()

	go func() {
		defer close(workerDoneCh)
		c.worker.Serve(workerCtx, c.handler, msgCh)
	}()

	c.logger.Printf("[DEBUG] consumer ready")
	var result error
	for {
		select {
		case c.readyCh <- struct{}{}:
			continue
		case <-chCloseCh:
			c.logger.Printf("[DEBUG] channel closed")
			result = errChannelClosed
		case err := <-c.connCloseCh:
			result = err
		case <-workerDoneCh:
			result = fmt.Errorf("workers unexpectedly stopped")
		case <-c.ctx.Done():
			result = nil
		}

		workerCancelFunc()
		<-workerDoneCh
		c.close(ch)
		return result
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

func (c *Consumer) close(ch Channel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			c.logger.Printf("[WARN] channel close: %s", err)
		}
	}
}
