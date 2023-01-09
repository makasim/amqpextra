package consumerpool

import (
	"context"
	"fmt"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	amqpextralogger "github.com/makasim/amqpextra/logger"
)

type Option func(p *Pool)

type logger interface {
	Printf(format string, v ...interface{})
}

func WithDecider(deciderFunc func(queueSize, poolSize, preFetch int) int) Option {
	return func(p *Pool) {
		p.deciderFunc = deciderFunc
	}
}

func WithMinSize(size int) Option {
	return func(p *Pool) {
		p.minSize = size
	}
}

func WithMaxSize(size int) Option {
	return func(p *Pool) {
		p.maxSize = size
	}
}

func WithLogger(l logger) Option {
	return func(p *Pool) {
		p.logger = l
	}
}

func WithInitCtx(ctx context.Context) Option {
	return func(p *Pool) {
		p.initCtx = ctx
	}
}

func WithConsumersPerConn(num int) Option {
	return func(p *Pool) {
		p.consumersPerConn = num
	}
}

type poolItem struct {
	dialer    *amqpextra.Dialer
	consumers []*consumer.Consumer
}

type Pool struct {
	dialerOptions   []amqpextra.Option
	consumerOptions []consumer.Option

	deciderFunc      func(queueSize, poolSize, preFetch int) int
	minSize          int
	maxSize          int
	consumersPerConn int
	consumerTotal    int
	initCtx          context.Context

	logger       logger
	decideDialer *amqpextra.Dialer
	items        []*poolItem
	closeCh      chan struct{}
}

func New(dialerOptions []amqpextra.Option, consumerOptions []consumer.Option, poolOptions []Option) (*Pool, error) {
	p := &Pool{
		dialerOptions:   dialerOptions,
		consumerOptions: consumerOptions,

		minSize: 1,
		maxSize: 10,
		closeCh: make(chan struct{}),
	}

	for _, opt := range poolOptions {
		opt(p)
	}

	if p.minSize <= 0 {
		return nil, fmt.Errorf("minSize must be greater than 0")
	}
	if p.maxSize == 0 {
		return nil, fmt.Errorf("maxSize must be greater than 0")
	}
	if p.minSize > p.maxSize {
		return nil, fmt.Errorf("minSize must be less or equal to maxSize")
	}

	if p.consumersPerConn == 0 {
		p.consumersPerConn = 1
	}
	if p.consumersPerConn <= 0 {
		return nil, fmt.Errorf("consumersPerConn must be greater than 0")
	}

	if p.deciderFunc == nil {
		p.deciderFunc = DefaultDeciderFunc()
	}
	if p.logger == nil {
		p.logger = amqpextralogger.Discard
	}
	if p.initCtx == nil {
		var initCtxCancel context.CancelFunc
		p.initCtx, initCtxCancel = context.WithTimeout(context.Background(), time.Second*5)
		defer initCtxCancel()
	}

	decideDialer, err := amqpextra.NewDialer(p.dialerOptions...)
	if err != nil {
		return nil, fmt.Errorf("dialer: new: %s", err)
	}
	p.decideDialer = decideDialer

	conn, err := decideDialer.Connection(p.initCtx)
	if err != nil {
		return nil, fmt.Errorf("dialer: connection: %s", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("dialer: channel: %s", err)
	}
	defer ch.Close()

	d, err := amqpextra.NewDialer(p.dialerOptions...)
	if err != nil {
		return nil, fmt.Errorf("dialer: new: %s", err)
	}

	c, err := d.Consumer(p.consumerOptions...)
	if err != nil {
		return nil, fmt.Errorf("consumer: new: %s", err)
	}

	cStateCh := c.Notify(make(chan consumer.State, 1))
	var cReady *consumer.Ready
loop:
	for {
		select {
		case state := <-cStateCh:
			if state.Ready != nil {
				cReady = state.Ready
				break loop
			}
		case <-p.initCtx.Done():
			p.decideDialer.Close()
			c.Close()
			return nil, fmt.Errorf("consumer: wait ready: %s", p.initCtx.Err())
		}
	}

	if !cReady.DeclareQueue {
		return nil, fmt.Errorf("consumer pool can work with declared queue only")
	}

	// first time queue declare synchronously to catch mismatched params
	if _, err = ch.QueueDeclare(
		cReady.Queue,
		cReady.DeclareDurable,
		cReady.DeclareAutoDelete,
		cReady.DeclareExclusive,
		cReady.DeclareNoWait,
		cReady.DeclareArgs,
	); err != nil {
		return nil, fmt.Errorf("channel: queue declare: %s", err)
	}

	p.items = append(p.items, &poolItem{
		dialer:    d,
		consumers: []*consumer.Consumer{c},
	})
	p.consumerTotal++

	if p.consumerTotal < p.minSize {
		p.startConsumersN(p.minSize - p.consumerTotal)
	}

	go p.decideConnectState(cReady)

	return p, nil
}

func (p *Pool) Close() {
	p.decideDialer.Close()
}

func (p *Pool) NotifyClosed() <-chan struct{} {
	return p.closeCh
}

func (p *Pool) decideConnectState(cReady *consumer.Ready) {
	defer close(p.closeCh)

	connCh := p.decideDialer.ConnectionCh()
	for conn := range connCh {
		p.decideConnectedState(conn, cReady)
	}

	for _, item := range p.items {
		for _, c := range item.consumers {
			c.Close()
		}
	}

	for _, item := range p.items {
		for _, c := range item.consumers {
			<-c.NotifyClosed()
		}
	}

	for _, item := range p.items {
		item.dialer.Close()
	}

	for _, item := range p.items {
		<-item.dialer.NotifyClosed()
	}
}

func (p *Pool) decideConnectedState(conn *amqpextra.Connection, cReady *consumer.Ready) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	ch, err := conn.AMQPConnection().Channel()
	if err != nil {
		p.logger.Printf("[ERROR] new channel failed", err)
		return
	}

	for {
		select {
		case <-t.C:
			q, err := ch.QueueDeclare(
				cReady.Queue,
				cReady.DeclareDurable,
				cReady.DeclareAutoDelete,
				cReady.DeclareExclusive,
				cReady.DeclareNoWait,
				cReady.DeclareArgs,
			)
			if err != nil {
				p.logger.Printf("[ERROR] queue declare failed", err)
				continue
			}
			queueSize := q.Messages

			newPoolSize := p.deciderFunc(queueSize, p.consumerTotal, cReady.PrefetchCount)

			diff := newPoolSize - p.consumerTotal
			if diff > 0 {
				if startedN := p.startConsumersN(diff); startedN > 0 {
					p.logger.Printf(fmt.Sprintf("[INFO] increased pool by %d; queue: %d; new len: %d", startedN, queueSize, len(p.items)))
				}
			} else if diff < 0 {
				if stoppedN := p.stopConsumersN(-diff); stoppedN > 0 {
					p.logger.Printf(fmt.Sprintf("[INFO] decreased pool by %d; new len: %d", stoppedN, len(p.items)))
				}
			}
		case <-conn.NotifyLost():
			return
		}
	}
}

func DefaultDeciderFunc() func(queueSize, poolSize, preFetch int) int {
	var emptyCounter int
	var growCounter int
	var prevQueueSize int

	return func(queueSize, poolSize, preFetch int) int {
		if queueSize == 0 {
			emptyCounter++
		} else {
			emptyCounter = 0
		}

		if queueSize > prevQueueSize {
			growCounter++
		} else {
			growCounter = 0
		}

		prevQueueSize = queueSize

		if growCounter > 3 || queueSize > (poolSize*preFetch) {
			return poolSize + 1
		} else if emptyCounter > 20 {
			emptyCounter -= 5
			return poolSize - 1
		}

		return poolSize
	}
}

func (p *Pool) startConsumersN(n int) int {
	if n <= 0 {
		return 0
	}

	if (p.consumerTotal + n) >= p.maxSize {
		n = p.maxSize - p.consumerTotal
	}

	lastItem := p.items[len(p.items)-1]

	var startedN int
	for i := 0; i < n; i++ {
		if len(lastItem.consumers) >= p.consumersPerConn {
			d, err := amqpextra.NewDialer(p.dialerOptions...)
			if err != nil {
				p.logger.Printf("[ERROR] new dialer failed", err)
				continue
			}

			lastItem = &poolItem{
				dialer: d,
			}
			p.items = append(p.items, lastItem)
		}

		c, err := lastItem.dialer.Consumer(p.consumerOptions...)
		if err != nil {
			p.logger.Printf("[ERROR] start consumer failed", err)
			continue
		}

		lastItem.consumers = append(lastItem.consumers, c)
		p.consumerTotal++
		startedN++
	}

	return startedN
}

func (p *Pool) stopConsumersN(n int) int {
	if n <= 0 {
		return 0
	}

	if (p.consumerTotal - n) <= p.minSize {
		n = p.consumerTotal - p.minSize
	}

	lastItem := p.items[len(p.items)-1]

	var stoppedN int
	for i := 0; i < n; i++ {
		// todo: wait for close
		lastItem.consumers[len(lastItem.consumers)-1].Close()
		<-lastItem.consumers[len(lastItem.consumers)-1].NotifyClosed()

		lastItem.consumers = lastItem.consumers[:len(lastItem.consumers)-1]
		p.consumerTotal--
		stoppedN++

		if len(lastItem.consumers) == 0 {
			lastItem.dialer.Close()
			<-lastItem.dialer.NotifyClosed()

			p.items = p.items[:len(p.items)-1]

			if len(p.items) == 0 {
				return stoppedN
			}

			lastItem = p.items[len(p.items)-1]
		}
	}

	return stoppedN
}
