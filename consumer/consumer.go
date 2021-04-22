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

type State struct {
	Unready *Unready
	Ready   *Ready
}

type Ready struct {
	Queue string
}

type Unready struct {
	Err error
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

	nextRetryPeriod func(attemptNumber int) time.Duration
	initFunc        func(conn AMQPConnection) (AMQPChannel, error)
	ctx             context.Context
	cancelFunc      context.CancelFunc
	logger          logger.Logger
	closeCh         chan struct{}

	mu       sync.Mutex
	stateChs []chan State

	internalStateCh chan State

	prefetchCount int
	qosGlobal     bool

	exchange   string
	routingKey string

	queue             string
	queueDeclare      bool
	declareDurable    bool
	declareAutoDelete bool
	declareExclusive  bool
	declareNoWait     bool
	declareArgs       amqp.Table

	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table

	retryCounter *retryCounter
}

func New(
	connCh <-chan *Connection,
	opts ...Option,
) (*Consumer, error) {
	c := &Consumer{
		connCh:          connCh,
		internalStateCh: make(chan State),
		prefetchCount:   1,

		closeCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	for _, stateCh := range c.stateChs {
		if stateCh == nil {
			return nil, fmt.Errorf("state chan must be not nil")
		}

		if cap(stateCh) == 0 {
			return nil, fmt.Errorf("state chan is unbuffered")
		}
	}

	if c.ctx != nil {
		c.ctx, c.cancelFunc = context.WithCancel(c.ctx)
	} else {
		c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	}

	if c.nextRetryPeriod == nil {
		c.nextRetryPeriod = func(_ int) time.Duration {
			return time.Second * 5
		}
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

	if c.queue == "" && c.exchange == "" && !c.queueDeclare {
		return nil, fmt.Errorf("WithQueue or WithExchange or WithDeclareQueue or WithTmpQueue options must be set")
	}

	if c.initFunc == nil {
		c.initFunc = func(conn AMQPConnection) (AMQPChannel, error) {
			return conn.(*amqp.Connection).Channel()
		}
	}

	ch := make(chan State, 2)
	rc := newRetryCounter(c.ctx, ch)
	c.retryCounter = rc
	c.stateChs = append(c.stateChs, rc.ch)

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
	return WithRetryPeriodFunc(func(_ int) time.Duration {
		return dur
	})
}

func WithRetryPeriodFunc(durFunc func(retryCount int) time.Duration) Option {
	return func(c *Consumer) {
		c.nextRetryPeriod = durFunc
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

func WithNotify(stateCh chan State) Option {
	return func(c *Consumer) {
		c.stateChs = append(c.stateChs, stateCh)
	}
}

func WithExchange(exchange, routingKey string) Option {
	return func(c *Consumer) {
		c.resetSource()
		c.exchange = exchange
		c.routingKey = routingKey
		c.declareAutoDelete = true
		c.queueDeclare = true
	}
}

func WithQueue(queue string) Option {
	return func(c *Consumer) {
		c.resetSource()
		c.queue = queue
	}
}

func WithTmpQueue() Option {
	return func(c *Consumer) {
		c.resetSource()
		c.queueDeclare = true
		c.declareAutoDelete = true
	}
}

func WithDeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) Option {
	return func(c *Consumer) {
		c.resetSource()
		c.queue = name
		c.queueDeclare = true
		c.declareDurable = durable
		c.declareAutoDelete = autoDelete
		c.declareExclusive = exclusive
		c.declareNoWait = noWait
		c.declareArgs = args
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

func (c *Consumer) resetSource() {
	c.queue = ""
	c.queueDeclare = false
	c.declareDurable = false
	c.declareAutoDelete = false
	c.declareExclusive = false
	c.declareNoWait = false
	c.declareArgs = nil
	c.routingKey = ""
	c.exchange = ""
}

func (c *Consumer) Notify(stateCh chan State) <-chan State {
	if cap(stateCh) == 0 {
		panic("state chan is unbuffered")
	}

	select {
	case state := <-c.internalStateCh:
		stateCh <- state
	case <-c.NotifyClosed():
		return stateCh
	}

	c.mu.Lock()
	c.stateChs = append(c.stateChs, stateCh)
	c.mu.Unlock()

	return stateCh
}

func (c *Consumer) NotifyClosed() <-chan struct{} {
	return c.closeCh
}

func (c *Consumer) Close() {
	c.cancelFunc()
}

func (c *Consumer) connectionState() {
	defer c.cancelFunc()
	defer close(c.closeCh)
	defer c.logger.Printf("[DEBUG] consumer stopped")

	c.logger.Printf("[DEBUG] consumer starting")

	state := State{Unready: &Unready{Err: amqp.ErrClosed}}
	for {
		select {
		case c.internalStateCh <- state:
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
				state = c.notifyUnready(err)
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
		if c.queueDeclare {
			q, declareErr := ch.QueueDeclare(c.queue, c.declareDurable, c.declareAutoDelete, c.declareExclusive, c.declareNoWait, c.declareArgs)
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
			c.notifyUnready(err)
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

	state := c.notifyReady(queue)

	go func() {
		defer close(workerDoneCh)
		c.worker.Serve(workerCtx, c.handler, msgCh)
	}()

	var result error

	for {
		select {
		case c.internalStateCh <- state:
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
	timer := time.NewTimer(c.nextRetryPeriod(c.retryCounter.read()))
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()
	state := c.notifyUnready(err)
	for {
		select {
		case c.internalStateCh <- state:
			continue
		case <-timer.C:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Consumer) notifyReady(queue string) State {
	state := State{
		Ready: &Ready{Queue: queue},
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stateCh := range c.stateChs {
		select {
		case stateCh <- state:
		case <-stateCh:
			stateCh <- state
		}
	}
	return state
}

func (c *Consumer) notifyUnready(err error) State {
	state := State{
		Unready: &Unready{Err: err},
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stateCh := range c.stateChs {
		select {
		case stateCh <- state:
		case <-stateCh:
			stateCh <- state
		}
	}
	return state
}

func (c *Consumer) close(ch AMQPChannel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			c.logger.Printf("[WARN] channel close: %s", err)
		}
	}
}
