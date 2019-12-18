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
	connCh   <-chan *amqp.Connection
	closeCh  <-chan *amqp.Error
	ctx      context.Context
	initFunc func(conn *amqp.Connection) (*amqp.Channel, error)

	logger       *logger
	publishingCh chan publishing
}

func NewPublisher(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	ctx context.Context,
	initFunc func(conn *amqp.Connection) (*amqp.Channel, error),
) *Publisher {
	p := &Publisher{
		connCh:   connCh,
		closeCh:  closeCh,
		ctx:      ctx,
		initFunc: initFunc,

		logger:       &logger{},
		publishingCh: make(chan publishing),
	}

	go p.worker()

	return p
}

func (p *Publisher) SetDebugFunc(f func(format string, v ...interface{})) {
	p.logger.SetDebugFunc(f)
}

func (p *Publisher) SetErrorFunc(f func(format string, v ...interface{})) {
	p.logger.SetErrorFunc(f)
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
	var wg sync.WaitGroup
L1:
	for conn := range p.connCh {
		select {
		case <-p.ctx.Done():
			break L1
		default:
		}

		ch, err := p.initFunc(conn)
		if err != nil {
			p.logger.Errorf("init func: %s", err)

			select {
			case <-time.NewTimer(time.Second * 5).C:
				continue
			case <-p.ctx.Done():
				break L1
			}
		}

		p.logger.Debugf("publisher started")

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
				p.logger.Debugf("publisher stopped")

				continue L1
			case <-p.ctx.Done():
				break L1
			}
		}
	}

	wg.Wait()

	p.logger.Debugf("publisher stopped")
}
