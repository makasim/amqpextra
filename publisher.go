package amqpextra

import (
	"context"
	"fmt"
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
	connCh       <-chan *amqp.Connection
	closeCh      <-chan *amqp.Error
	ctx          context.Context
	initFunc     func(conn *amqp.Connection) (*amqp.Channel, error)
	logger       Logger
	publishingCh chan publishing
}

func NewPublisher(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	ctx context.Context,
	initFunc func(conn *amqp.Connection) (*amqp.Channel, error),
	logger Logger,
) *Publisher {
	if logger == nil {
		logger = nilLogger
	}

	p := &Publisher{
		connCh:   connCh,
		closeCh:  closeCh,
		ctx:      ctx,
		initFunc: initFunc,

		logger:       logger,
		publishingCh: make(chan publishing),
	}

	go p.worker()

	return p
}

func (p *Publisher) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) <-chan error {
	doneCh := make(chan error, 1)
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
		doneCh <- fmt.Errorf("publisher closed")

		return doneCh
	case p.publishingCh <- publishing:
		return doneCh
	}
}

func (p *Publisher) worker() {

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
					publishing.DoneCh <- ch.Publish(
						publishing.Exchange,
						publishing.Queue,
						publishing.Mandatory,
						publishing.Immediate,
						publishing.Publishing,
					)
				case <-p.closeCh:
					p.logger.Printf("[DEBUG] publisher stopped")

					continue L1
				case <-p.ctx.Done():
					p.logger.Printf("[DEBUG] publisher stopped")

					break L1
				}
			}
		case <-p.ctx.Done():
			break L1
		}
	}
}
