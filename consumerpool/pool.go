package consumerpool

import (
	"context"
	"fmt"
	"time"

	"github.com/makasim/amqpextra"
	amqpextraconsumer "github.com/makasim/amqpextra/consumer"
	amqpextralogger "github.com/makasim/amqpextra/logger"
)

type Option func(p *Pool)

type logger interface {
	Printf(format string, v ...interface{})
}

type consumer interface {
	Close()
	NotifyClosed() <-chan struct{}
	Notify(chan amqpextraconsumer.State) <-chan amqpextraconsumer.State
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
		p.l = l
	}
}

func WithInitCtx(ctx context.Context) Option {
	return func(p *Pool) {
		p.initCtx = ctx
	}
}

type Pool struct {
	dialerOptions   []amqpextra.Option
	consumerOptions []amqpextraconsumer.Option

	deciderFunc func(queueSize, poolSize, preFetch int) int
	minSize     int
	maxSize     int
	initCtx     context.Context

	l       logger
	d       *amqpextra.Dialer
	cs      []consumer
	closeCh chan struct{}
}

func New(dialerOptions []amqpextra.Option, consumerOptions []amqpextraconsumer.Option, poolOptions []Option) (*Pool, error) {
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
	if p.l == nil {
		p.l = amqpextralogger.Discard
	}
	if p.initCtx == nil {
		var initCtxCancel context.CancelFunc
		p.initCtx, initCtxCancel = context.WithTimeout(context.Background(), time.Second*5)
		defer initCtxCancel()
	}

	d, err := amqpextra.NewDialer(p.dialerOptions...)
	if err != nil {
		return nil, fmt.Errorf("dialer: new: %s", err)
	}
	p.d = d

	conn, err := d.Connection(p.initCtx)
	if err != nil {
		return nil, fmt.Errorf("dialer: connection: %s", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("dialer: channel: %s", err)
	}
	defer ch.Close()

	c, err := startConsumer(p.dialerOptions, p.consumerOptions)
	if err != nil {
		return nil, fmt.Errorf("consumer: new: %s", err)
	}

	cStateCh := c.Notify(make(chan amqpextraconsumer.State, 1))
	var cReady *amqpextraconsumer.Ready
loop:
	for {
		select {
		case state := <-cStateCh:
			if state.Ready != nil {
				cReady = state.Ready
				break loop
			}
		case <-p.initCtx.Done():
			p.d.Close()
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

	p.cs = append(p.cs, c)

	go p.connectState(cReady)

	return p, nil
}

func (p *Pool) Close() {
	p.d.Close()
}

func (p *Pool) NotifyClosed() <-chan struct{} {
	return p.closeCh
}

func (p *Pool) connectState(cReady *amqpextraconsumer.Ready) {
	defer close(p.closeCh)

	connCh := p.d.ConnectionCh()
	for conn := range connCh {
		p.connectedState(conn, cReady)
	}

	for _, c := range p.cs {
		c.Close()
	}

	for _, c := range p.cs {
		<-c.NotifyClosed()
	}
}

func (p *Pool) connectedState(conn *amqpextra.Connection, cReady *amqpextraconsumer.Ready) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	ch, err := conn.AMQPConnection().Channel()
	if err != nil {
		p.l.Printf("[ERROR] new channel failed", err)
		return
	}

	for {
		select {
		case <-t.C:
			if len(p.cs) < p.minSize {
				diff := p.minSize - len(p.cs)
				for i := 0; i < diff; i++ {
					c, err := startConsumer(p.dialerOptions, p.consumerOptions)
					if err != nil {
						p.l.Printf("[ERROR] start consumer failed", err)
						continue
					}

					p.cs = append(p.cs, c)
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
				p.l.Printf("[ERROR] queue declare failed", err)
				continue
			}

			poolSize := len(p.cs)
			queueSize := q.Messages

			newPoolSize := p.deciderFunc(queueSize, poolSize, cReady.PrefetchCount)

			diff := newPoolSize - poolSize

			if diff > 0 && poolSize < p.maxSize {
				for i := 0; i < diff; i++ {
					c, err := startConsumer(p.dialerOptions, p.consumerOptions)
					if err != nil {
						p.l.Printf("[ERROR] start consumer failed", err)
						continue
					}

					p.cs = append(p.cs, c)

					if len(p.cs) >= p.maxSize {
						break
					}
				}
				p.l.Printf(fmt.Sprintf("[INFO] increase pool size by %d, new len: %d", diff, len(p.cs)))
			} else if diff < 0 && poolSize > p.minSize {
				for i := 0; i < -diff; i++ {
					// todo: wait for close
					p.cs[len(p.cs)-1].Close()
					p.cs = p.cs[:len(p.cs)-1]

					if len(p.cs) <= p.minSize {
						break
					}
				}

				p.l.Printf(fmt.Sprintf("[INFO] decrease pool size by %d, new len: %d", -diff, len(p.cs)))
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
			growCounter = 0
			return poolSize + 1
		} else if emptyCounter > 20 {
			emptyCounter -= 3
			return poolSize - 1
		}

		return poolSize
	}
}

func startConsumer(dialerOptions []amqpextra.Option, consumerOptions []amqpextraconsumer.Option) (consumer, error) {
	d, err := amqpextra.NewDialer(dialerOptions...)
	if err != nil {
		return nil, fmt.Errorf("dialer: new: %s", err)
	}

	c, err := d.Consumer(consumerOptions...)
	if err != nil {
		return nil, fmt.Errorf("consumer: new: %s", err)
	}

	return c, nil
}
