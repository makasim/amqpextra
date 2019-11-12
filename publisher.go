package amqpextra

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
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
	connCh     <-chan *amqp.Connection
	closeCh    <-chan *amqp.Error
	doneCh     <-chan struct{}
	initFunc   func(conn *amqp.Connection) (*amqp.Channel, error)
	logErrFunc func(format string, v ...interface{})
	logDbgFunc func(format string, v ...interface{})

	publishingCh chan publishing
}

func NewPublisher(
	connCh <-chan *amqp.Connection,
	closeCh <-chan *amqp.Error,
	doneCh  <-chan struct{},
	initFunc func(conn *amqp.Connection) (*amqp.Channel, error),
	logErrFunc func(format string, v ...interface{}),
	logDbgFunc func(format string, v ...interface{}),
) *Publisher {
	p := &Publisher{
		connCh:     connCh,
		closeCh:    closeCh,
		doneCh:     doneCh,
		initFunc:   initFunc,
		logErrFunc: logErrFunc,
		logDbgFunc: logDbgFunc,

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
	case <-p.doneCh:
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
		case <-p.doneCh:
			break L1
		default:
		}

		ch, err := p.initFunc(conn)
		if err != nil {
			p.logErrFunc("init func: %s", err)
			time.Sleep(time.Second * 5)

			continue
		}

		p.logDbgFunc("publisher started")

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
				p.logDbgFunc("publisher stopped")

				continue L1
			case <-p.doneCh:
				break L1
			}
		}
	}

	wg.Wait()

	p.logDbgFunc("publisher stopped")
}
