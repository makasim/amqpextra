package amqpextra

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Publishing struct {
	Context   context.Context
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	WaitReady bool
	Message   amqp.Publishing
	ResultCh  chan error
}

type Publisher struct {
	connCh  <-chan *amqp.Connection
	closeCh <-chan *amqp.Error

	once         sync.Once
	started      bool
	ctx          context.Context
	cancelFunc   context.CancelFunc
	restartSleep time.Duration
	initFunc     func(conn *amqp.Connection) (*amqp.Channel, error)
	logger       Logger
	publishingCh chan Publishing
	doneCh       chan struct{}
	readyCh      chan struct{}
	unreadyCh    chan struct{}
}

func NewPublisher(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
) *Publisher {
	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Publisher{
		connCh:  connCh,
		closeCh: closeCh,

		started:      false,
		ctx:          ctx,
		cancelFunc:   cancelFunc,
		logger:       nilLogger,
		restartSleep: time.Second * 5,
		publishingCh: make(chan Publishing),
		doneCh:       make(chan struct{}),
		readyCh:      make(chan struct{}),
		unreadyCh:    make(chan struct{}),
		initFunc: func(conn *amqp.Connection) (*amqp.Channel, error) {
			return conn.Channel()
		},
	}
}

func (p *Publisher) SetLogger(logger Logger) {
	if !p.started {
		p.logger = logger
	}
}

func (p *Publisher) SetContext(ctx context.Context) {
	if !p.started {
		p.ctx, p.cancelFunc = context.WithCancel(ctx)
	}
}

func (p *Publisher) SetRestartSleep(d time.Duration) {
	if !p.started {
		p.restartSleep = d
	}
}

func (p *Publisher) SetInitFunc(f func(conn *amqp.Connection) (*amqp.Channel, error)) {
	if !p.started {
		p.initFunc = f
	}
}

func (p *Publisher) Start() {
	p.once.Do(func() {
		p.started = true
		go p.connect()
	})
}

func (p *Publisher) Run() {
	p.Start()

	<-p.doneCh
}

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) Publish(msg Publishing) {
	p.Start()

	if msg.ResultCh != nil && cap(msg.ResultCh) == 0 {
		panic("amqpextra: resultCh channel is unbuffered")
	}

	if msg.Context == nil {
		msg.Context = context.Background()
	}

	unreadyCh := p.Unready()
	if msg.WaitReady {
		unreadyCh = nil
	}

	select {
	case p.publishingCh <- msg:
	case <-msg.Context.Done():
		if msg.ResultCh != nil {
			msg.ResultCh <- msg.Context.Err()
		} else {
			p.logger.Printf("[ERROR] msg context done: %s", msg.Context.Err())
		}
	// noinspection GoNilness
	case <-unreadyCh:
		if msg.ResultCh != nil {
			msg.ResultCh <- fmt.Errorf("publisher not ready")
		} else {
			p.logger.Printf("[ERROR] publisher not ready")
		}
	case <-p.ctx.Done():
		if msg.ResultCh != nil {
			msg.ResultCh <- fmt.Errorf("publisher stopped")
		} else {
			p.logger.Printf("[ERROR] publisher stopped")
		}
	}
}

func (p *Publisher) Ready() <-chan struct{} {
	p.Start()

	return p.readyCh
}

func (p *Publisher) Unready() <-chan struct{} {
	return p.unreadyCh
}

func (p *Publisher) connect() {
	defer close(p.doneCh)

	for {
		select {
		case conn, ok := <-p.connCh:
			if !ok {
				p.close(nil)

				return
			}

			select {
			case <-p.closeCh:
				continue
			case <-p.ctx.Done():
				return
			default:
			}

			p.logger.Printf("[DEBUG] publisher started")
			if !p.serve(conn) {
				return
			}
		case p.unreadyCh <- struct{}{}:
		case <-p.ctx.Done():
			p.close(nil)

			return
		}
	}
}

func (p *Publisher) serve(conn *amqp.Connection) bool {
	ch, err := p.initFunc(conn)
	if err != nil {
		p.logger.Printf("[ERROR] init func: %s", err)

		timer := time.NewTimer(time.Second * 5)

		select {
		case <-timer.C:
			return true
		case <-p.ctx.Done():
			timer.Stop()
			p.close(nil)

			return false
		}
	}

	for {
		select {
		case p.readyCh <- struct{}{}:
		case publishing := <-p.publishingCh:
			p.publish(ch, publishing)
		case <-p.closeCh:
			p.logger.Printf("[DEBUG] publisher stopped")

			return true
		case <-p.ctx.Done():
			p.logger.Printf("[DEBUG] publisher stopped")

			p.close(ch)

			return false
		}
	}
}

func (p *Publisher) publish(ch *amqp.Channel, msg Publishing) {
	select {
	case <-msg.Context.Done():
		if msg.ResultCh != nil {
			msg.ResultCh <- msg.Context.Err()
		} else {
			p.logger.Printf("[ERROR] msg context done: %s", msg.Context.Err())
		}
	default:
	}

	result := ch.Publish(
		msg.Exchange,
		msg.Key,
		msg.Mandatory,
		msg.Immediate,
		msg.Message,
	)

	if msg.ResultCh != nil {
		msg.ResultCh <- result
	} else if result != nil {
		p.logger.Printf("[ERROR] publish: %s", result)
	}
}

func (p *Publisher) close(ch *amqp.Channel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			p.logger.Printf("[WARN] publisher: channel close: %s", err)
		}
	}

	close(p.readyCh)
}
