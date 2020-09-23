package amqpextra

import (
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func NewPublisher(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	opts ...publisher.Option,
) *publisher.Publisher {
	connCh := make(chan publisher.Connection)
	connCloseCh := make(chan *amqp.Error, 1)

	p := publisher.New(connCh, connCloseCh, opts...)
	go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, p.Closed())

	return p
}

func proxyPublisherConn(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	connCh chan publisher.Connection,
	connCloseCh chan *amqp.Error,
	publisherCloseCh <-chan struct{},
) {
	go func() {
		defer close(connCh)

		for conn := range amqpConnCh {
			select {
			case connCh <- &publisher.AMQP{Conn: conn}:
				continue
			case <-publisherCloseCh:
				return
			}
		}
	}()

	go func() {
		defer close(connCloseCh)

		for err := range amqpConnCloseCh {
			select {
			case connCloseCh <- err:
				continue
			case <-publisherCloseCh:
				return
			default:
			}
		}
	}()
}
