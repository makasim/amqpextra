package consumerpool

import (
	"context"
	"fmt"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
)

type Option func(p *Pool)

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

func WithLogger(l logger.Logger) Option {
	return func(p *Pool) {
		p.l = l
	}
}

type Pool struct {
	dialerOptions   []amqpextra.Option
	consumerOptions []consumer.Option

	deciderFunc func(queueSize, poolSize, preFetch int) int
	minSize     int
	maxSize     int

	l  logger.Logger
	d  *amqpextra.Dialer
	cs []*consumer.Consumer
}

func New(dialerOptions []amqpextra.Option, consumerOptions []consumer.Option, poolOptions []Option) (*Pool, error) {
	p := &Pool{
		dialerOptions:   dialerOptions,
		consumerOptions: consumerOptions,

		minSize: 1,
		maxSize: 10,
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
		p.l = logger.Discard
	}

	d, err := amqpextra.NewDialer(p.dialerOptions...)
	if err != nil {
		return nil, fmt.Errorf("dialer: new: %s", err)
	}
	p.d = d

	conn, err := d.Connection(context.TODO())
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
			// todo: add timeout
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

func (p *Pool) Shutdown(ctx context.Context) error {
	p.d.Close()

	select {
	case <-p.d.NotifyClosed():
		for _, c := range p.cs {
			select {
			case <-c.NotifyClosed():
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) connectState(cReady *consumer.Ready) {
	connCh := p.d.ConnectionCh()

	for {
		select {
		case conn, ok := <-connCh:
			if !ok {
				for _, c := range p.cs {
					c.Close()
				}

				return
			}

			p.connectedState(conn, cReady)
		}
	}
}

func (p *Pool) connectedState(conn *amqpextra.Connection, cReady *consumer.Ready) {
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
			} else if diff < 0 && poolSize > p.minSize {
				for i := 0; i < -diff; i++ {
					// todo: wait for close
					p.cs[len(p.cs)-1].Close()
					p.cs = p.cs[:len(p.cs)-1]

					if len(p.cs) <= p.minSize {
						break
					}
				}
			}
		case <-conn.NotifyLost():
			return
		}
	}
}

func DefaultDeciderFunc() func(queueSize, poolSize, preFetch int) int {
	var emptyCounter int

	return func(queueSize, poolSize, preFetch int) int {
		newPoolSize := poolSize

		if queueSize < (poolSize*preFetch - int(float64(poolSize*preFetch)*0.2)) {
			emptyCounter += 1
		}

		if queueSize > (poolSize*preFetch + int(float64(poolSize*preFetch)*0.2)) {
			emptyCounter = 0
			newPoolSize += 1
		} else if emptyCounter > 5 {
			emptyCounter = 0
			newPoolSize -= 1
		}

		return newPoolSize
	}
}

func startConsumer(dialerOptions []amqpextra.Option, consumerOptions []consumer.Option) (*consumer.Consumer, error) {
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
