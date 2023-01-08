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

type Pool struct {
	dialerOptions   []amqpextra.Option
	consumerOptions []consumer.Option

	deciderFunc func(queueSize, poolSize, preFetch int) int
	minSize     int
	maxSize     int
	initCtx     context.Context

	logger       logger
	decideDialer *amqpextra.Dialer
	consumers    []*consumer.Consumer
	dialers      []*amqpextra.Dialer
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

	d, c, err := startConsumer(p.dialerOptions, p.consumerOptions)
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

	p.consumers = append(p.consumers, c)
	p.dialers = append(p.dialers, d)

	go p.connectState(cReady)

	return p, nil
}

func (p *Pool) Close() {
	p.decideDialer.Close()
}

func (p *Pool) NotifyClosed() <-chan struct{} {
	return p.closeCh
}

func (p *Pool) connectState(cReady *consumer.Ready) {
	defer close(p.closeCh)

	connCh := p.decideDialer.ConnectionCh()
	for conn := range connCh {
		p.connectedState(conn, cReady)
	}

	for _, c := range p.consumers {
		c.Close()
	}

	for _, c := range p.consumers {
		<-c.NotifyClosed()
	}

	for _, d := range p.dialers {
		d.Close()
	}

	for _, d := range p.dialers {
		<-d.NotifyClosed()
	}
}

func (p *Pool) connectedState(conn *amqpextra.Connection, cReady *consumer.Ready) {
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
			if len(p.consumers) < p.minSize {
				diff := p.minSize - len(p.consumers)
				for i := 0; i < diff; i++ {
					d, c, err := startConsumer(p.dialerOptions, p.consumerOptions)
					if err != nil {
						p.logger.Printf("[ERROR] start consumer failed", err)
						continue
					}

					p.consumers = append(p.consumers, c)
					p.dialers = append(p.dialers, d)
				}
			}

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

			poolSize := len(p.consumers)
			queueSize := q.Messages

			newPoolSize := p.deciderFunc(queueSize, poolSize, cReady.PrefetchCount)

			diff := newPoolSize - poolSize

			if diff > 0 && poolSize < p.maxSize {
				for i := 0; i < diff; i++ {
					d, c, err := startConsumer(p.dialerOptions, p.consumerOptions)
					if err != nil {
						p.logger.Printf("[ERROR] start consumer failed", err)
						continue
					}

					p.consumers = append(p.consumers, c)
					p.dialers = append(p.dialers, d)

					if len(p.consumers) >= p.maxSize {
						break
					}
				}
				p.logger.Printf(fmt.Sprintf("[INFO] increase pool by %d; queue: %d; new len: %d", diff, queueSize, len(p.consumers)))
			} else if diff < 0 && poolSize > p.minSize {
				for i := 0; i < -diff; i++ {
					// todo: wait for close
					p.consumers[len(p.consumers)-1].Close()
					p.consumers = p.consumers[:len(p.consumers)-1]

					if len(p.consumers) <= p.minSize {
						break
					}
				}

				p.logger.Printf(fmt.Sprintf("[INFO] decrease pool by %d; new len: %d", -diff, len(p.consumers)))
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

func startConsumer(dialerOptions []amqpextra.Option, consumerOptions []consumer.Option) (*amqpextra.Dialer, *consumer.Consumer, error) {
	d, err := amqpextra.NewDialer(dialerOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("dialer: new: %s", err)
	}

	c, err := d.Consumer(consumerOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: new: %s", err)
	}

	return d, c, nil
}
