package publisher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

type Option func(p *Publisher)

type Message struct {
	Context    context.Context
	Exchange   string
	Key        string
	Mandatory  bool
	Immediate  bool
	WaitReady  bool
	Publishing amqp.Publishing
	ResultCh   chan error
}

type Publisher struct {
	connCh      <-chan Connection
	connCloseCh <-chan *amqp.Error

	ctx          context.Context
	cancelFunc   context.CancelFunc
	restartSleep time.Duration
	initFunc     func(conn Connection) (Channel, error)
	logger       logger.Logger
	publishingCh chan Message
	closeCh      chan struct{}
	readyCh      chan struct{}
	unreadyCh    chan struct{}
}

func New(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	opts ...Option,
) *Publisher {
	connCh := make(chan Connection)
	connCloseCh := make(chan *amqp.Error, 1)

	p := New2(connCh, connCloseCh, opts...)

	go func() {
		defer close(connCh)
		defer close(connCloseCh)

		for {
			select {
			case conn, ok := <-amqpConnCh:
				if !ok {
					return
				}

				select {
				case connCh <- &AMQP{Conn: conn}:
				case <-p.closeCh:
					return
				}

				select {
				case err := <-amqpConnCloseCh:
					connCloseCh <- err
				case <-p.closeCh:
					return
				}
			case <-p.closeCh:
				return
			}
		}
	}()

	return p
}

func New2(
	connCh <-chan Connection,
	connCloseCh <-chan *amqp.Error,
	opts ...Option,
) *Publisher {
	p := &Publisher{
		connCh:      connCh,
		connCloseCh: connCloseCh,

		publishingCh: make(chan Message),
		closeCh:      make(chan struct{}),
		readyCh:      make(chan struct{}),
		unreadyCh:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.ctx != nil {
		p.ctx, p.cancelFunc = context.WithCancel(p.ctx)
	} else {
		p.ctx, p.cancelFunc = context.WithCancel(context.Background())
	}

	if p.restartSleep == 0 {
		p.restartSleep = time.Second * 5
	}

	if p.logger == nil {
		p.logger = logger.Discard
	}

	if p.initFunc == nil {
		p.initFunc = func(conn Connection) (Channel, error) {
			return conn.Channel()
		}
	}

	go p.unreadyState()

	return p
}

func WithLogger(l logger.Logger) Option {
	return func(p *Publisher) {
		p.logger = l
	}
}

func WithContext(ctx context.Context) Option {
	return func(p *Publisher) {
		p.ctx = ctx
	}
}

func WithRestartSleep(dur time.Duration) Option {
	return func(p *Publisher) {
		p.restartSleep = dur
	}
}

func WithInitFunc(f func(conn Connection) (Channel, error)) Option {
	return func(p *Publisher) {
		p.initFunc = f
	}
}

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) Publish(msg Message) {
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
	case <-p.closeCh:
		p.reply(msg.ResultCh, fmt.Errorf("publisher stopped"))
		return
	default:
	}

	select {
	case p.publishingCh <- msg:
	case <-msg.Context.Done():
		p.reply(msg.ResultCh, fmt.Errorf("message: %v", msg.Context.Err()))
	// noinspection GoNilness
	case <-unreadyCh:
		p.reply(msg.ResultCh, fmt.Errorf("publisher not ready"))
	case <-p.ctx.Done():
		p.reply(msg.ResultCh, fmt.Errorf("publisher stopped"))
	}
}

func (p *Publisher) reply(resultCh chan error, result error) {
	if resultCh != nil {
		resultCh <- result
	} else if result != nil {
		p.logger.Printf("[ERROR] %v", result)
	}
}

func (p *Publisher) Ready() <-chan struct{} {
	return p.readyCh
}

func (p *Publisher) Unready() <-chan struct{} {
	return p.unreadyCh
}

func (p *Publisher) Closed() <-chan struct{} {
	return p.closeCh
}

func (p *Publisher) unreadyState() {
	defer close(p.closeCh)

	for {
		select {
		case conn, ok := <-p.connCh:
			if !ok {
				p.close(nil)

				return
			}

			select {
			case <-p.connCloseCh:
				continue
			case <-p.ctx.Done():
				return
			default:
			}

			p.logger.Printf("[DEBUG] publisher started")
			if !p.readyState(conn) {
				return
			}
		case p.unreadyCh <- struct{}{}:
		case <-p.ctx.Done():
			p.close(nil)

			return
		}
	}
}

func (p *Publisher) readyState(conn Connection) bool {
	ch, err := p.initFunc(conn)
	if err != nil {
		p.logger.Printf("[ERROR] init func: %s", err)

		timer := time.NewTimer(p.restartSleep)

		select {
		case <-timer.C:
			return true
		case <-p.ctx.Done():
			timer.Stop()
			p.close(nil)

			return false
		}
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	p.logger.Printf("[DEBUG] publisher ready")
	for {
		select {
		case p.readyCh <- struct{}{}:
		case <-closeCh:
			p.logger.Printf("[DEBUG] channel closed")

			return true
		case msg := <-p.publishingCh:
			p.publish(ch, msg)
		case <-p.connCloseCh:
			p.logger.Printf("[DEBUG] publisher stopped")

			return true
		case <-p.ctx.Done():
			p.close(ch)

			p.logger.Printf("[DEBUG] publisher stopped")

			return false
		}
	}
}

func (p *Publisher) publish(ch Channel, msg Message) {
	select {
	case <-msg.Context.Done():
		p.reply(msg.ResultCh, fmt.Errorf("message: %v", msg.Context.Err()))
	default:
	}

	result := ch.Publish(
		msg.Exchange,
		msg.Key,
		msg.Mandatory,
		msg.Immediate,
		msg.Publishing,
	)

	p.reply(msg.ResultCh, result)
}

func (p *Publisher) close(ch Channel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			p.logger.Printf("[WARN] publisher: channel close: %s", err)
		}
	}

	close(p.unreadyCh)
}
