package amqpextra

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var errChannelClosed = fmt.Errorf("channel closed")

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
	retryPeriod  time.Duration
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
		retryPeriod:  time.Second * 5,
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
		go p.connectionState()
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

func (p *Publisher) connectionState() {
	defer close(p.doneCh)
	defer p.logger.Printf("[DEBUG] publisher stopped")

	p.logger.Printf("[DEBUG] publisher starting")
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

			if err := p.channelState(conn); err == nil {
				return
			}

			p.logger.Printf("[DEBUG] publisher unready")
		case p.unreadyCh <- struct{}{}:
		case <-p.ctx.Done():
			p.close(nil)

			return
		}
	}
}

func (p *Publisher) channelState(conn *amqp.Connection) error {
	for {
		ch, err := p.initFunc(conn)
		if err != nil {
			p.logger.Printf("[ERROR] init func: %s", err)
			return p.retry(err)
		}

		if err := p.publishState(ch); err != errChannelClosed {
			return err
		}
	}
}

func (p *Publisher) publishState(ch *amqp.Channel) error {
	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	p.logger.Printf("[DEBUG] publisher ready")
	for {
		select {
		case p.readyCh <- struct{}{}:
		case msg := <-p.publishingCh:
			p.publish(ch, msg)
		case <-chCloseCh:
			p.logger.Printf("[DEBUG] channel closed")
			return errChannelClosed
		case <-p.ctx.Done():
			p.close(ch)
			return nil
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

func (p *Publisher) retry(err error) error {
	timer := time.NewTimer(p.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()

	for {
		select {
		case p.unreadyCh <- struct{}{}:
			continue
		case <-timer.C:
			return err
		case <-p.ctx.Done():
			return nil
		}
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
