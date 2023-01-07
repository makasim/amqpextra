package consumerpool

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
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

type Pool struct {
	dialerOptions   []amqpextra.Option
	consumerOptions []consumer.Option

	deciderFunc func(queueSize, poolSize, preFetch int) int

	minSize int
	maxSize int
}

func New(dialerOptions []amqpextra.Option, consumerOptions []consumer.Option, poolOptions []Option) (*Pool, error) {
	p := &Pool{
		dialerOptions:   dialerOptions,
		consumerOptions: consumerOptions,

		deciderFunc: DefaultDeciderFunc(),

		minSize: 1,
		maxSize: 10,
	}

	for _, opt := range poolOptions {
		opt(p)
	}

	// todo: validate options

	// amqpextra.WithURL("amqp://guest:guest@localhost:5672/test")
	d, err := amqpextra.NewDialer(p.dialerOptions...)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := d.Connection(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	consumerPool := make([]*consumer.Consumer, 0)

	var queueReady *consumer.Ready
	for i := 0; i < p.minSize; i++ {
		c, err := startConsumer(p.dialerOptions, p.consumerOptions)
		if err != nil {
			// todo: stop started consumers
			return nil, err
		}

		if queueReady == nil {
			for cState := range c.Notify(make(chan consumer.State, 1)) {
				if cState.Ready != nil {
					queueReady = cState.Ready
					break
				}
			}
		}

		consumerPool = append(consumerPool, c)
	}

	if !queueReady.DeclareQueue {
		return nil, fmt.Errorf("consumer pool can work with declared queue only")
	}

	go func() {
		prefetch := queueReady.PrefetchCount

		t := time.NewTicker(time.Second)
		for {
			<-t.C

			q, err := ch.QueueDeclare(
				queueReady.Queue,
				queueReady.DeclareDurable,
				queueReady.DeclareAutoDelete,
				queueReady.DeclareExclusive,
				queueReady.DeclareNoWait,
				queueReady.DeclareArgs,
			)
			if err != nil {
				log.Fatal(err)
			}

			poolSize := len(consumerPool)
			queueSize := q.Messages

			newPoolSize := p.deciderFunc(queueSize, poolSize, prefetch)

			diff := newPoolSize - poolSize

			if diff > 0 && poolSize < p.maxSize {
				for i := 0; i < diff; i++ {
					c, err := startConsumer(p.dialerOptions, p.consumerOptions)
					if err != nil {
						log.Fatal(err)
					}

					consumerPool = append(consumerPool, c)

					if len(consumerPool) >= p.maxSize {
						break
					}
				}
			} else if diff < 0 && poolSize > p.minSize {
				for i := 0; i < -diff; i++ {
					consumerPool[len(consumerPool)-1].Close()
					consumerPool = consumerPool[:len(consumerPool)-1]

					if len(consumerPool) <= p.minSize {
						break
					}
				}
			}
		}
	}()

	return p, nil
}

func (p *Pool) Close() {

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
