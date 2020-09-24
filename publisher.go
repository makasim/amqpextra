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

//nolint:dupl // ignore linter err
func proxyPublisherConn(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	connCh chan publisher.Connection,
	connCloseCh chan *amqp.Error,
	publisherCloseCh <-chan struct{},
) {
	go func() {
		defer close(connCh)

		for {
			select {
			case conn, ok := <-amqpConnCh:
				if !ok {
					return
				}

				select {
				case connCh <- &publisher.AMQP{Conn: conn}:
				case <-publisherCloseCh:
					return
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()

	go func() {
		defer close(connCloseCh)

		for {
			select {
			case err, ok := <-amqpConnCloseCh:
				if !ok {
					return
				}

				select {
				case connCloseCh <- err:
				default:
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()
}
