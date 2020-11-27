// Package amqpextra provides Dialer for dialing in case the connection lost.
package amqpextra

import (
	"context"
	"errors"
	"sync"
	"time"

	"fmt"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

type State struct {
	Ready   *Ready
	Unready *Unready
}

type Ready struct{}

type Unready struct {
	Err error
}

// Option could be used to configure Dialer
type Option func(c *Dialer)

// AMQPConnection is an interface for streadway's *amqp.Connection
type AMQPConnection interface {
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Close() error
}

// Connection provides access to streadway's *amqp.Connection as well as notification channels
// A notification indicates that something wrong has happened to the connection.
// The client should get a fresh connection from Dialer.
type Connection struct {
	amqpConn AMQPConnection
	lostCh   chan struct{}
}

// AMQPConnection returns streadway's *amqp.Connection
func (c *Connection) AMQPConnection() *amqp.Connection {
	return c.amqpConn.(*amqp.Connection)
}

// NotifyLost notifies when current connection is lost and new once should be requested
func (c *Connection) NotifyLost() chan struct{} {
	return c.lostCh
}

type config struct {
	amqpUrls   []string
	amqpDial   func(url string, c amqp.Config) (AMQPConnection, error)
	amqpConfig amqp.Config

	logger      logger.Logger
	retryPeriod time.Duration
	ctx         context.Context
}

// Dialer is responsible for keeping the connection up.
// If connection is lost or closed. It tries dial a server again and again with some wait periods.
// Dialer keep connection up until it Dialer.Close() method called or the context is canceled.
type Dialer struct {
	config

	ctx        context.Context
	cancelFunc context.CancelFunc

	connCh chan *Connection

	mu       sync.Mutex
	stateChs []chan State

	internalStateChan chan State

	closedCh chan struct{}
}

// Dial returns established connection or an error.
// It keeps retrying until timeout 30sec is reached.
func Dial(opts ...Option) (*amqp.Connection, error) {
	d, err := NewDialer(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()

	return d.Connection(ctx)
}

// NewDialer returns Dialer or a configuration error.
func NewDialer(opts ...Option) (*Dialer, error) {
	c := &Dialer{
		config: config{
			amqpUrls: make([]string, 0, 1),
			amqpDial: func(url string, c amqp.Config) (AMQPConnection, error) {
				return amqp.DialConfig(url, c)
			},
			amqpConfig: amqp.Config{
				Heartbeat: time.Second * 30,
				Locale:    "en_US",
			},

			retryPeriod: time.Second * 5,
			logger:      logger.Discard,
		},
		internalStateChan: make(chan State),
		connCh:            make(chan *Connection),
		closedCh:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	for _, readyCh := range c.stateChs {
		if readyCh == nil {
			return nil, errors.New("state chan must be not nil")
		}

		if cap(readyCh) == 0 {
			return nil, errors.New("state chan is unbuffered")
		}
	}

	if len(c.amqpUrls) == 0 {
		return nil, fmt.Errorf("url(s) must be set")
	}

	for _, url := range c.amqpUrls {
		if url == "" {
			return nil, fmt.Errorf("url(s) must be not empty")
		}
	}

	if c.config.ctx != nil {
		c.ctx, c.cancelFunc = context.WithCancel(c.config.ctx)
	} else {
		c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	}

	if c.retryPeriod <= 0 {
		return nil, fmt.Errorf("retryPeriod must be greater then zero")
	}

	go c.connectState()

	return c, nil
}

// WithURL configure RabbitMQ servers to dial.
// Dialer dials url by round-robbin
func WithURL(urls ...string) Option {
	return func(c *Dialer) {
		c.amqpUrls = append(c.amqpUrls, urls...)
	}
}

// WithAMQPDial configure dial function.
// The function takes the url and amqp.Config and returns AMQPConnection.
func WithAMQPDial(dial func(url string, c amqp.Config) (AMQPConnection, error)) Option {
	return func(c *Dialer) {
		c.amqpDial = dial
	}
}

// WithLogger configure the logger used by Dialer
func WithLogger(l logger.Logger) Option {
	return func(c *Dialer) {
		c.logger = l
	}
}

// WithLogger configure Dialer context
// The context could used later to stop Dialer
func WithContext(ctx context.Context) Option {
	return func(c *Dialer) {
		c.config.ctx = ctx
	}
}

// WithRetryPeriod configure how much time to wait before next dial attempt. Default: 5sec.
func WithRetryPeriod(dur time.Duration) Option {
	return func(c *Dialer) {
		c.retryPeriod = dur
	}
}

// WithConnectionProperties configure connection properties set on dial.
func WithConnectionProperties(props amqp.Table) Option {
	return func(c *Dialer) {
		c.amqpConfig.Properties = props
	}
}

// WithNotify helps subscribe on Dialer ready/unready events.
func WithNotify(stateCh chan State) Option {
	return func(c *Dialer) {
		c.stateChs = append(c.stateChs, stateCh)
	}
}

// ConnectionCh returns Connection channel.
// The channel should be used to get established connections.
// The client must subscribe on Connection.NotifyLost().
// Once lost, client must stop using current connection and get new one from Connection channel.
// Connection channel is closed when Dialer is closed. Don't forget to check for closed connection.
func (c *Dialer) ConnectionCh() <-chan *Connection {
	return c.connCh
}

// Notify could be used to subscribe on Dialer ready/unready events
func (c *Dialer) Notify(stateCh chan State) <-chan State {
	if cap(stateCh) == 0 {
		panic("state chan is unbuffered")
	}

	select {
	case state := <-c.internalStateChan:
		stateCh <- state
	case <-c.NotifyClosed():
		return stateCh
	}

	c.mu.Lock()
	c.stateChs = append(c.stateChs, stateCh)
	c.mu.Unlock()

	return stateCh
}

// NotifyClosed could be used to subscribe on Dialer closed event.
// Dialer.ConnectionCh() could no longer be used after this point
func (c *Dialer) NotifyClosed() <-chan struct{} {
	return c.closedCh
}

// Close initiate Dialer close.
// Subscribe Dialer.NotifyClosed() to know when it was finally closed.
func (c *Dialer) Close() {
	c.cancelFunc()
}

// Connection returns streadway's *amqp.Connection.
// The client should subscribe on Dialer.NotifyReady(), Dialer.NotifyUnready() events in order to know when the connection is lost.
func (c *Dialer) Connection(ctx context.Context) (*amqp.Connection, error) {
	select {
	case <-c.ctx.Done():
		return nil, fmt.Errorf("connection closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-c.connCh:
		if !ok {
			return nil, fmt.Errorf("connection closed")
		}

		return conn.AMQPConnection(), nil
	}
}

// Consumer returns a consumer that support reconnection feature.
func (c *Dialer) Consumer(opts ...consumer.Option) (*consumer.Consumer, error) {
	opts = append([]consumer.Option{
		consumer.WithLogger(c.logger),
		consumer.WithContext(c.ctx),
	}, opts...)

	return NewConsumer(c.ConnectionCh(), opts...)
}

// Publisher returns a consumer that support reconnection feature.
func (c *Dialer) Publisher(opts ...publisher.Option) (*publisher.Publisher, error) {
	opts = append([]publisher.Option{
		publisher.WithLogger(c.logger),
		publisher.WithContext(c.ctx),
	}, opts...)

	return NewPublisher(c.ConnectionCh(), opts...)
}

// connectState is a starting point.
// It chooses URL and dials the server.
// Once connection is established it pass control to Dialer.connectedState()
// If connection is closed or lost Dialer.connectedState() returns control back to Dialer.connectState()
// Exits on Dialer.Close()
func (c *Dialer) connectState() {
	defer close(c.connCh)
	defer close(c.closedCh)
	defer c.cancelFunc()
	defer c.logger.Printf("[DEBUG] connection closed")

	i := 0
	l := len(c.amqpUrls)

	c.logger.Printf("[DEBUG] connection unready")
	state := State{Unready: &Unready{Err: amqp.ErrClosed}}
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		url := c.amqpUrls[i]
		i = (i + 1) % l

		connCh := make(chan AMQPConnection)
		errorCh := make(chan error)

		go func() {
			c.logger.Printf("[DEBUG] dialing")
			if conn, err := c.amqpDial(url, c.amqpConfig); err != nil {
				errorCh <- err
			} else {
				connCh <- conn
			}
		}()

	loop2:
		for {
			select {
			case c.internalStateChan <- state:
				continue
			case conn := <-connCh:
				select {
				case <-c.ctx.Done():
					c.closeConn(conn)
					return
				default:
				}

				if err := c.connectedState(conn); err != nil {
					c.logger.Printf("[DEBUG] connection unready: %s", err)
					state = c.notifyUnready(err)
					break loop2
				}

				return
			case err := <-errorCh:
				c.logger.Printf("[DEBUG] connection unready: %v", err)
				if retryErr := c.waitRetry(err); retryErr != nil {
					state = State{Unready: &Unready{Err: retryErr}}
					break loop2
				}

				return
			}
		}
	}
}

// connectedState serves Dialer.ConnectionCh() and Dialer.Connection() methods.
// It shares an established connection with all the clients who requests it.
// Once connection is lost or closed it gives control back to Dialer.connectState().
func (c *Dialer) connectedState(amqpConn AMQPConnection) error {
	defer c.closeConn(amqpConn)

	lostCh := make(chan struct{})
	defer close(lostCh)
	internalCloseCh := amqpConn.NotifyClose(make(chan *amqp.Error, 1))

	conn := &Connection{amqpConn: amqpConn, lostCh: lostCh}
	c.logger.Printf("[DEBUG] connection ready")
	state := c.notifyReady()
	for {
		select {
		case c.internalStateChan <- state:
			continue
		case c.connCh <- conn:
			continue
		case err, ok := <-internalCloseCh:
			if !ok {
				err = amqp.ErrClosed
			}
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Dialer) notifyUnready(err error) State {
	state := State{Unready: &Unready{Err: err}}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stateCh := range c.stateChs {
		select {
		case stateCh <- state:
		case <-stateCh:
			stateCh <- state
		}
	}
	return state
}

func (c *Dialer) notifyReady() State {
	state := State{Ready: &Ready{}}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stateCh := range c.stateChs {
		select {
		case stateCh <- state:
		case <-stateCh:
			stateCh <- state
		}
	}
	return state
}

func (c *Dialer) waitRetry(err error) error {
	timer := time.NewTimer(c.retryPeriod)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}()
	state := c.notifyUnready(err)
	for {
		select {
		case c.internalStateChan <- state:
			continue
		case <-timer.C:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *Dialer) closeConn(conn AMQPConnection) {
	if err := conn.Close(); err == amqp.ErrClosed {
		return
	} else if err != nil {
		c.logger.Printf("[ERROR] %s", err)
	}
}
