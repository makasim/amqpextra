package publisher

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

type AMQPConnection interface {
}

type AMQPChannel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyFlow(c chan bool) chan bool
	Close() error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Confirm(noWait bool) error
}

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
	connCh <-chan *Connection

	ctx         context.Context
	cancelFunc  context.CancelFunc
	retryPeriod time.Duration
	initFunc    func(conn AMQPConnection) (AMQPChannel, error)
	logger      logger.Logger

	mu         sync.Mutex
	readyChs   []chan struct{}
	unreadyChs []chan error

	confirmation       bool
	confirmationBuffer uint

	closeCh chan struct{}

	publishingCh chan Message

	internalUnreadyCh chan error
	internalReadyCh   chan struct{}
}

func New(
	connCh <-chan *Connection,
	opts ...Option,
) (*Publisher, error) {
	p := &Publisher{
		connCh: connCh,

		publishingCh:      make(chan Message),
		closeCh:           make(chan struct{}),
		internalUnreadyCh: make(chan error),
		internalReadyCh:   make(chan struct{}),
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

	for _, unreadyCh := range p.unreadyChs {
		if unreadyCh == nil {
			return nil, fmt.Errorf("unready chan must be not nil")
		}

		if cap(unreadyCh) == 0 {
			return nil, fmt.Errorf("unready chan is unbuffered")
		}
	}

	for _, readyCh := range p.readyChs {
		if readyCh == nil {
			return nil, fmt.Errorf("ready chan must be not nil")
		}

		if cap(readyCh) == 0 {
			return nil, fmt.Errorf("ready chan is unbuffered")
		}
	}

	if p.logger == nil {
		p.logger = logger.Discard
	}

	if p.confirmation && p.confirmationBuffer <= 1 {
		return nil, fmt.Errorf("confirmation buffer size must be greater than 1")
	}

	if p.initFunc == nil {
		p.initFunc = func(conn AMQPConnection) (AMQPChannel, error) {
			return conn.(*amqp.Connection).Channel()
		}
	}

	go p.connectionState()

	return p, nil
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

func WithInitFunc(f func(conn AMQPConnection) (AMQPChannel, error)) Option {
	return func(p *Publisher) {
		p.initFunc = f
	}
}

func WithNotify(readyCh chan struct{}, unreadyCh chan error) Option {
	return func(p *Publisher) {
		p.readyChs = append(p.readyChs, readyCh)
		p.unreadyChs = append(p.unreadyChs, unreadyCh)
	}
}

// WithConfirmation tells publisher to turn on publisher confirm mode.
// The buffer option tells how many messages might be in-flight.
// Once limit is reached no new messages could be published.
// The confirmation result is returned via msg.ResultCh.
func WithConfirmation(buffer uint) Option {
	return func(p *Publisher) {
		p.confirmationBuffer = buffer
		p.confirmation = true
	}
}

func (p *Publisher) Notify(readyCh chan struct{}, unreadyCh chan error) (ready <-chan struct{}, unready <-chan error) {
	if cap(readyCh) == 0 {
		panic("ready chan is unbuffered")
	}
	if cap(unreadyCh) == 0 {
		panic("unready chan is unbuffered")
	}

	select {
	case <-p.NotifyClosed():
		close(unreadyCh)
		return readyCh, unreadyCh
	default:
	}

	p.mu.Lock()
	p.readyChs = append(p.readyChs, readyCh)
	p.unreadyChs = append(p.unreadyChs, unreadyCh)
	p.mu.Unlock()

	select {
	case <-p.internalReadyCh:
		select {
		case readyCh <- struct{}{}:
		default:
		}

		return readyCh, unreadyCh
	case err, ok := <-p.internalUnreadyCh:
		if !ok {
			close(unreadyCh)
			return readyCh, unreadyCh
		}

		select {
		case unreadyCh <- err:
		default:
		}

		return readyCh, unreadyCh
	}
}

func (p *Publisher) Publish(msg Message) error {
	return <-p.Go(msg)
}

func (p *Publisher) Go(msg Message) <-chan error {
	if msg.ResultCh == nil {
		msg.ResultCh = make(chan error, 1)
	}
	if cap(msg.ResultCh) == 0 {
		panic("amqpextra: resultCh channel is unbuffered")
	}

	if msg.Context == nil {
		msg.Context = context.Background()
	}

	var unreadyCh <-chan error
	if msg.ErrOnUnready {
		unreadyCh = p.internalUnreadyCh
	}

	select {
	case <-p.closeCh:
		msg.ResultCh <- fmt.Errorf("publisher stopped")
		return msg.ResultCh
	default:
	}

	select {
	case p.publishingCh <- msg:
	case <-msg.Context.Done():
		msg.ResultCh <- fmt.Errorf("message: %v", msg.Context.Err())
	// noinspection GoNilness
	case <-unreadyCh:
		msg.ResultCh <- fmt.Errorf("publisher not ready")
	case <-p.ctx.Done():
		msg.ResultCh <- fmt.Errorf("publisher stopped")
	}

	return msg.ResultCh
}

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) NotifyClosed() <-chan struct{} {
	return p.closeCh
}

func (p *Publisher) connectionState() {
	defer p.cancelFunc()
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, unreadyCh := range p.unreadyChs {
			close(unreadyCh)
		}
	}()
	defer close(p.closeCh)
	defer p.logger.Printf("[DEBUG] publisher stopped")

	var connErr error = amqp.ErrClosed
	p.logger.Printf("[DEBUG] publisher starting")
	p.notifyUnready(connErr)
	for {
		select {
		case conn, ok := <-p.connCh:
			if !ok {
				return
			}
			select {
			case <-conn.NotifyClose():
				continue
			case <-p.ctx.Done():
				return
			default:
			}
			err := p.channelState(conn.AMQPConnection(), conn.NotifyClose())
			if err != nil {
				p.logger.Printf("[DEBUG] publisher unready")
				connErr = err
				continue
			}

			return
		case p.internalUnreadyCh <- connErr:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Publisher) channelState(conn AMQPConnection, connCloseCh <-chan struct{}) error {
	for {
		ch, err := p.initFunc(conn)
		if err != nil {
			p.logger.Printf("[ERROR] init func: %s", err)
			return p.waitRetry(err)
		}

		var resultChCh chan chan error

		if p.confirmation {
			err := ch.Confirm(false)
			if err != nil {
				return p.waitRetry(err)
			}

			confirmationCh := ch.NotifyPublish(make(chan amqp.Confirmation, p.confirmationBuffer))
			resultChCh = make(chan chan error, p.confirmationBuffer)

			go p.handleConfirmations(resultChCh, confirmationCh)
		}

		err = p.publishState(ch, connCloseCh, resultChCh)
		if err == errChannelClosed {
			continue
		}

		p.close(ch)
		return err
	}
}

func (p *Publisher) handleConfirmations(resultChCh chan chan error, confirmationCh chan amqp.Confirmation) {
	p.logger.Printf("[DEBUG] handle confirmation started")
	defer p.logger.Printf("[DEBUG] handle confirmation stopped")
	for {
		select {
		case c, ok := <-confirmationCh:
			if !ok {
				for {
					select {
					case resultCh := <-resultChCh:
						resultCh <- amqp.ErrClosed
						continue
					default:
						return
					}
				}
			}

			resultCh := <-resultChCh

			if c.Ack {
				resultCh <- nil
			} else {
				resultCh <- fmt.Errorf("not delivered")
			}
		}
	}
}

func (p *Publisher) publishState(ch AMQPChannel, connCloseCh <-chan struct{}, resultChCh chan chan error) error {
	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	chFlowCh := ch.NotifyFlow(make(chan bool, 1))

	p.logger.Printf("[DEBUG] publisher ready")
	p.notifyReady()
	for {
		select {
		case p.internalReadyCh <- struct{}{}:
			continue
		case msg := <-p.publishingCh:
			p.publish(ch, msg, resultChCh)
		case <-chCloseCh:
			p.logger.Printf("[DEBUG] channel closed")
			return errChannelClosed
		case <-connCloseCh:
			return amqp.ErrClosed
		case resume := <-chFlowCh:
			if resume {
				continue
			}
			if err := p.pausedState(chFlowCh, connCloseCh, chCloseCh); err != nil {
				return err
			}
			p.notifyReady()

		case <-p.ctx.Done():
			return nil
		}
	}
}

func (p *Publisher) pausedState(chFlowCh <-chan bool, connCloseCh <-chan struct{}, chCloseCh chan *amqp.Error) error {
	p.logger.Printf("[WARN] publisher flow paused")
	errFlowPaused := fmt.Errorf("publisher flow paused")
	p.notifyUnready(errFlowPaused)
	for {
		select {
		case p.internalUnreadyCh <- errFlowPaused:
			continue
		case resume := <-chFlowCh:
			if resume {
				p.logger.Printf("[INFO] publisher flow resumed")
				return nil
			}
		case <-chCloseCh:
			p.logger.Printf("[DEBUG] channel closed")
			return errChannelClosed
		case <-connCloseCh:
			return amqp.ErrClosed
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}
}

func (p *Publisher) publish(ch AMQPChannel, msg Message, resultChCh chan chan error) {
	select {
	case <-msg.Context.Done():
		msg.ResultCh <- fmt.Errorf("message: %v", msg.Context.Err())
	default:
	}

	result := ch.Publish(
		msg.Exchange,
		msg.Key,
		msg.Mandatory,
		msg.Immediate,
		msg.Publishing,
	)

	if !p.confirmation {
		msg.ResultCh <- result
		return
	}

	if result != nil {
		msg.ResultCh <- result
		return
	}

	select {
	case resultChCh <- msg.ResultCh:
	case <-p.ctx.Done():
		msg.ResultCh <- p.ctx.Err()
		return
	}
}

func (p *Publisher) waitRetry(err error) error {
	timer := time.NewTimer(p.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()

	p.notifyUnready(err)

	for {
		select {
		case p.internalUnreadyCh <- err:
			continue
		case <-timer.C:
			return err
		case <-p.ctx.Done():
			return nil
		}
	}
}

func (p *Publisher) notifyUnready(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ch := range p.unreadyChs {
		select {
		case ch <- err:
		default:
		}
	}
}

func (p *Publisher) notifyReady() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ch := range p.readyChs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (p *Publisher) close(ch AMQPChannel) {
	if ch != nil {
		if err := ch.Close(); err != nil && !strings.Contains(err.Error(), "channel/connection is not open") {
			p.logger.Printf("[WARN] publisher: channel close: %s", err)
		}
	}
}
