package publisher

import (
	"context"
	"errors"
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

	mu         *sync.Mutex
	readyChs   []chan struct{}
	unreadyChs []chan error

	closeCh chan struct{}

	publishingCh chan Message

	internalCh chan error
}

func New(
	connCh <-chan *Connection,
	opts ...Option,
) (*Publisher, error) {
	p := &Publisher{
		connCh: connCh,

		publishingCh: make(chan Message),
		internalCh:   make(chan error),
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
			return nil, errors.New("unready chan must be not nil")
		}

		if cap(unreadyCh) == 0 {
			return nil, errors.New("unready chan is unbuffered")
		}
	}

	for _, readyCh := range p.readyChs {
		if readyCh == nil {
			return nil, errors.New("ready chan must be not nil")
		}

		if cap(readyCh) == 0 {
			return nil, errors.New("ready chan is unbuffered")
		}
	}

	if p.logger == nil {
		p.logger = logger.Discard
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

func WithReadyCh(readyCh chan struct{}) Option {
	return func(p *Publisher) {
		p.readyChs = append(p.readyChs, readyCh)
	}
}

func WithUnreadyCh(unreadyCh chan error) Option {
	return func(p *Publisher) {
		p.unreadyChs = append(p.unreadyChs, unreadyCh)
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
		unreadyCh = p.internalCh
	}

	select {
	case <-p.closeCh:
		p.reply(msg.ResultCh, fmt.Errorf("publisher stopped"))
		return msg.ResultCh
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

	return msg.ResultCh
}

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) NotifyReady(readyCh chan struct{}) <-chan struct{} {
	if cap(readyCh) == 0 {
		panic("ready chan is unbuffered")
	}

	select {
	case <-p.NotifyClosed():
		return readyCh
	default:
		readyCh <- struct{}{}
		p.mu.Lock()
		p.readyChs = append(p.readyChs, readyCh)
		p.mu.Unlock()

		return readyCh
	}
}

func (p *Publisher) NotifyUnready(unreadyCh chan error) <-chan error {
	if cap(unreadyCh) == 0 {
		panic("unready chan is unbuffered")
	}

	select {
	case <-p.NotifyClosed():
		close(unreadyCh)
	default:
		p.mu.Lock()
		defer p.mu.Unlock()
		p.unreadyChs = append(p.unreadyChs, unreadyCh)
	}

	select {
	case err := <-p.internalCh:
		unreadyCh <- err
	}

	return unreadyCh
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
	defer p.logger.Printf("[DEBUG] publisher stopped")

	var connErr error = amqp.ErrClosed
	p.notifyUnready(connErr)
	p.logger.Printf("[DEBUG] publisher starting")
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
				p.notifyUnready(err)
				connErr = err
				continue
			}

			return
		case <-p.ctx.Done():
			p.close(nil)

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

		err = p.publishState(ch, connCloseCh)
		if err == errChannelClosed {
			continue
		}

		p.close(ch)
		return err
	}
}

func (p *Publisher) publishState(ch AMQPChannel, connCloseCh <-chan struct{}) error {
	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	chFlowCh := ch.NotifyFlow(make(chan bool, 1))

	p.logger.Printf("[DEBUG] publisher ready")
	p.notifyReady()
	for {
		select {
		case msg := <-p.publishingCh:
			p.publish(ch, msg)
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
		case p.internalCh <- errFlowPaused:
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

func (p *Publisher) publish(ch AMQPChannel, msg Message) {
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
		case p.internalCh <- err:
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
