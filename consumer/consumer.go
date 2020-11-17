package consumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

var errChannelClosed = fmt.Errorf("channel closed")

type Ready struct {
	Queue string
}

type AMQPConnection interface {
}

type AMQPChannel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Close() error
}

type Option func(c *Consumer)

type Consumer struct {
	handler Handler
	connCh  <-chan *Connection

	worker Worker

	retryPeriod time.Duration
	initFunc    func(conn AMQPConnection) (AMQPChannel, error)
	ctx         context.Context
	cancelFunc  context.CancelFunc
	logger      logger.Logger
	closeCh     chan struct{}

	mu         sync.Mutex
	unreadyChs []chan error
	readyChs   []chan Ready

	internalUnreadyCh chan error
	internalReadyCh   chan Ready

	prefetchCount int
	qosGlobal     bool

	exchange   string
	routingKey string

	queue        string
	queueDeclare bool

	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

func New(
	connCh <-chan *Connection,
	opts ...Option,
) (*Consumer, error) {
	c := &Consumer{
		connCh: connCh,

		internalUnreadyCh: make(chan error),
		internalReadyCh:   make(chan Ready),
		prefetchCount:     1,

		closeCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	for _, unreadyCh := range c.unreadyChs {
		if unreadyCh == nil {
			return nil, fmt.Errorf("unready chan must be not nil")
		}

		if cap(unreadyCh) == 0 {
			return nil, fmt.Errorf("unready chan is unbuffered")
		}
	}

	for _, readyCh := range c.readyChs {
		if readyCh == nil {
			return nil, fmt.Errorf("ready chan must be not nil")
		}

		if cap(readyCh) == 0 {
			return nil, fmt.Errorf("ready chan is unbuffered")
		}
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

	if c.worker == nil {
		c.worker = &DefaultWorker{Logger: c.logger}
	}

	if c.handler == nil {
		return nil, fmt.Errorf("handler must be not nil")
	}

	if c.queue == "" && c.exchange == "" {
		return nil, fmt.Errorf("WithQueue or WithExchange options must be set")
	}

	if c.queue != "" && c.exchange != "" && !c.queueDeclare {
		return nil, fmt.Errorf("only one of WithQueue or WithExchange options must be set")
	}

	if c.initFunc == nil {
		c.initFunc = func(conn AMQPConnection) (AMQPChannel, error) {
			return conn.(*amqp.Connection).Channel()
		}
	}

	go c.connectionState()

	return c, nil
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

func WithInitFunc(f func(conn AMQPConnection) (AMQPChannel, error)) Option {
	return func(c *Consumer) {
		c.initFunc = f
	}
}

func WithWorker(w Worker) Option {
	return func(c *Consumer) {
		c.worker = w
	}
}

func WithQos(prefetchCount int, global bool) Option {
	return func(c *Consumer) {
		c.prefetchCount = prefetchCount
		c.qosGlobal = global
	}
}

func WithNotify(readyCh chan Ready, unreadyCh chan error) Option {
	return func(c *Consumer) {
		c.readyChs = append(c.readyChs, readyCh)
		c.unreadyChs = append(c.unreadyChs, unreadyCh)
	}
}

func WithExchange(exchange, routingKey string) Option {
	return func(c *Consumer) {
		c.exchange = exchange
		c.routingKey = routingKey
	}
}

func WithQueue(queue string, declare bool) Option {
	return func(c *Consumer) {
		c.queue = queue
		c.queueDeclare = declare
	}
}

func WithHandler(h Handler) Option {
	return func(c *Consumer) {
		c.handler = h
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

func (c *Consumer) Notify(readyCh chan Ready, unreadyCh chan error) (ready <-chan Ready, unready <-chan error) {
	if cap(readyCh) == 0 {
		panic("ready chan is unbuffered")
	}
	if cap(unreadyCh) == 0 {
		panic("unready chan is unbuffered")
	}

	select {
	case <-c.NotifyClosed():
		close(unreadyCh)
		return readyCh, unreadyCh
	default:
	}

	c.mu.Lock()
	c.readyChs = append(c.readyChs, readyCh)
	c.unreadyChs = append(c.unreadyChs, unreadyCh)
	c.mu.Unlock()

	select {
	case ready := <-c.internalReadyCh:
		select {
		case readyCh <- ready:
		default:
		}

		return readyCh, unreadyCh
	case err, ok := <-c.internalUnreadyCh:
		if !ok {
			close(unreadyCh)
			return readyCh, unreadyCh
		}

		select {
		case unreadyCh <- err:
		default:
		}

		return readyCh, unreadyCh
	}
}

func (c *Consumer) NotifyClosed() <-chan struct{} {
	return c.closeCh
}

func (c *Consumer) Close() {
	c.cancelFunc()
}

func (c *Consumer) connectionState() {
	defer c.cancelFunc()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, unreadyCh := range c.unreadyChs {
			close(unreadyCh)
		}
	}()
	defer close(c.closeCh)
	defer c.logger.Printf("[DEBUG] consumer stopped")

	c.logger.Printf("[DEBUG] consumer starting")
	var connErr error = amqp.ErrClosed
	c.notifyUnready(connErr)
	for {
		select {
		case c.internalUnreadyCh <- connErr:
			continue
		case conn, ok := <-c.connCh:
			if !ok {
				return
			}

			select {
			case <-conn.NotifyClose():
				continue
			case <-c.ctx.Done():
				return
			default:
			}

			if err := c.channelState(conn.AMQPConnection(), conn.NotifyClose()); err != nil {
				c.logger.Printf("[DEBUG] consumer unready")
				connErr = err
				c.notifyUnready(connErr)
				continue
			}

			c.logger.Printf("[DEBUG] consumer unready")
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) channelState(conn AMQPConnection, connCloseCh <-chan struct{}) error {
	for {
		ch, err := c.initFunc(conn)
		if err != nil {
			c.logger.Printf("[ERROR] init func: %s", err)
			return c.waitRetry(err)
		}

		err = ch.Qos(c.prefetchCount, 0, c.qosGlobal)
		if err != nil {
			return c.waitRetry(err)
		}

		queue := c.queue
		if c.queueDeclare || c.queue == "" {
			q, declareErr := ch.QueueDeclare(queue, false, false, true, false, nil)
			if declareErr != nil {
				return c.waitRetry(declareErr)
			}
			queue = q.Name
		}

		if c.exchange != "" {
			err = ch.QueueBind(queue, c.routingKey, c.exchange, false, nil)
			if err != nil {
				return c.waitRetry(err)
			}
		}

		err = c.consumeState(ch, queue, connCloseCh)
		if err == errChannelClosed {
			continue
		}

		return err
	}
}

func (c *Consumer) consumeState(ch AMQPChannel, queue string, connCloseCh <-chan struct{}) error {

	msgCh, err := ch.Consume(
		queue,
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
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	workerDoneCh := make(chan struct{})
	workerCtx, workerCancelFunc := context.WithCancel(c.ctx)
	defer workerCancelFunc()

	c.logger.Printf("[DEBUG] consumer ready")

	c.notifyReady(queue)

	go func() {
		defer close(workerDoneCh)
		c.worker.Serve(workerCtx, c.handler, msgCh)
	}()

	var result error

	for {
		select {
		case c.internalReadyCh <- Ready{Queue: queue}:
			continue
		case <-cancelCh:
			c.logger.Printf("[DEBUG] consumption canceled")
			result = fmt.Errorf("consumption canceled")
		case <-chCloseCh:
			c.logger.Printf("[DEBUG] channel closed")
			result = errChannelClosed
		case <-connCloseCh:
			result = amqp.ErrClosed
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

	c.notifyUnready(err)

	for {
		select {
		case c.internalUnreadyCh <- err:
			continue
		case <-timer.C:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Consumer) notifyUnready(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.unreadyChs {
		select {
		case ch <- err:
		default:
		}
	}
}

func (c *Consumer) notifyReady(queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.readyChs {
		select {
		case ch <- Ready{Queue: queue}:
		default:
		}
	}
}

func (c *Consumer) close(ch AMQPChannel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			c.logger.Printf("[WARN] channel close: %s", err)
		}
	}
}
