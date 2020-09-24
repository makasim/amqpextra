package publisher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

var errChannelClosed = fmt.Errorf("channel closed")

type Option func(p *Publisher)

type Message struct {
	Context      context.Context
	Exchange     string
	Key          string
	Mandatory    bool
	Immediate    bool
	ErrOnUnready bool
	Publishing   amqp.Publishing
	ResultCh     chan error
}

type Publisher struct {
	connCh      <-chan Connection
	connCloseCh <-chan *amqp.Error

	ctx          context.Context
	cancelFunc   context.CancelFunc
	retryPeriod  time.Duration
	initFunc     func(conn Connection) (Channel, error)
	logger       logger.Logger
	publishingCh chan Message
	closeCh      chan struct{}
	readyCh      chan struct{}
	unreadyCh    chan error
}

func New(
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
		unreadyCh:    make(chan error),
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.ctx != nil {
		p.ctx, p.cancelFunc = context.WithCancel(p.ctx)
	} else {
		p.ctx, p.cancelFunc = context.WithCancel(context.Background())
	}

	if p.retryPeriod == 0 {
		p.retryPeriod = time.Second * 5
	}

	if p.logger == nil {
		p.logger = logger.Discard
	}

	if p.initFunc == nil {
		p.initFunc = func(conn Connection) (Channel, error) {
			return conn.Channel()
		}
	}

	go p.connectionState()

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
		p.retryPeriod = dur
	}
}

func WithInitFunc(f func(conn Connection) (Channel, error)) Option {
	return func(p *Publisher) {
		p.initFunc = f
	}
}

func (p *Publisher) Publish(msg Message) {
	if msg.ResultCh != nil && cap(msg.ResultCh) == 0 {
		panic("amqpextra: resultCh channel is unbuffered")
	}

	if msg.Context == nil {
		msg.Context = context.Background()
	}

	var unreadyCh <-chan error
	if msg.ErrOnUnready {
		unreadyCh = p.Unready()
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

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) Ready() <-chan struct{} {
	return p.readyCh
}

func (p *Publisher) Unready() <-chan error {
	return p.unreadyCh
}

func (p *Publisher) Closed() <-chan struct{} {
	return p.closeCh
}

func (p *Publisher) connectionState() {
	defer p.cancelFunc()
	defer close(p.unreadyCh)
	defer close(p.closeCh)
	defer p.logger.Printf("[DEBUG] publisher stopped")

	var connErr error = amqp.ErrClosed

	p.logger.Printf("[DEBUG] publisher starting")
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

			err := p.channelState(conn)
			if err != nil {
				p.logger.Printf("[DEBUG] publisher unready")

				connErr = err
				continue
			}

			return

		case p.unreadyCh <- connErr:
		case <-p.ctx.Done():
			p.close(nil)

			return
		}
	}
}

func (p *Publisher) channelState(conn Connection) error {
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

func (p *Publisher) publishState(ch Channel) error {
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
		case err := <-p.connCloseCh:
			return err
		case <-p.ctx.Done():
			p.close(ch)
			return nil
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

func (p *Publisher) reply(resultCh chan error, result error) {
	if resultCh != nil {
		resultCh <- result
	} else if result != nil {
		p.logger.Printf("[ERROR] %v", result)
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
		case p.unreadyCh <- err:
			continue
		case <-timer.C:
			return err
		case <-p.ctx.Done():
			return nil
		}
	}
}

func (p *Publisher) close(ch Channel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			p.logger.Printf("[WARN] publisher: channel close: %s", err)
		}
	}
}
