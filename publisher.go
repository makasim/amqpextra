package amqpextra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type publishing struct {
	Exchange   string
	Queue      string
	Mandatory  bool
	Immediate  bool
	Publishing amqp.Publishing
	DoneCh     chan error
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
	publishingCh chan publishing
	doneCh       chan struct{}
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
		publishingCh: make(chan publishing),
		doneCh:       make(chan struct{}),
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
		go p.start()
	})
}

func (p *Publisher) Run() {
	p.Start()

	<-p.doneCh
}

func (p *Publisher) Close() {
	p.cancelFunc()
}

func (p *Publisher) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing, doneCh chan error) {
	p.Start()

	publishing := publishing{
		Exchange:   exchange,
		Queue:      key,
		Mandatory:  mandatory,
		Immediate:  immediate,
		Publishing: msg,
		DoneCh:     doneCh,
	}

	select {
	case <-p.ctx.Done():
		p.logger.Printf("[ERROR] cannot publish to stopped publisher")

		if doneCh != nil {
			doneCh <- fmt.Errorf("publisher stopped")
		}
	case p.publishingCh <- publishing:
	}
}

func (p *Publisher) start() {
	defer close(p.doneCh)

L1:
	for {
		select {
		case conn, ok := <-p.connCh:
			if !ok {
				break L1
			}

			select {
			case <-p.closeCh:
				continue L1
			default:
			}

			ch, err := p.initFunc(conn)
			if err != nil {
				p.logger.Printf("[ERROR] init func: %s", err)

				select {
				case <-time.NewTimer(time.Second * 5).C:
					continue L1
				case <-p.ctx.Done():
					break L1
				}
			}

			p.logger.Printf("[DEBUG] publisher started")

			for {
				select {
				case publishing := <-p.publishingCh:
					result := ch.Publish(
						publishing.Exchange,
						publishing.Queue,
						publishing.Mandatory,
						publishing.Immediate,
						publishing.Publishing,
					)

					if publishing.DoneCh != nil {
						go func(err error, doneCh chan error) {
							select {
							case doneCh <- result:
							case <-time.NewTimer(time.Second * 5).C:
								p.logger.Printf("[WARN] publish result has not been read out from doneCh within safeguard time. Make sure you are reading from the channel.")
							}
						}(result, publishing.DoneCh)
					}
				case <-p.closeCh:
					p.logger.Printf("[DEBUG] publisher stopped")

					continue L1
				case <-p.ctx.Done():
					p.logger.Printf("[DEBUG] publisher stopped")

					if err := ch.Close(); err != nil {
						p.logger.Printf("[WARN] channel close: %s", err)
					}

					break L1
				}
			}
		case <-p.ctx.Done():
			break L1
		}
	}
}
