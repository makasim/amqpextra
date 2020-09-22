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
	defer close(connCloseCh)
	defer close(connCh)

	for {
		select {
		case conn, ok := <-amqpConnCh:
			if !ok {
				return
			}

			select {
			case connCh <- &publisher.AMQP{Conn: conn}:
			case err := <-amqpConnCloseCh:
				select {
				case connCloseCh <- err:
				default:
				}

				continue
			case <-publisherCloseCh:
				return
			}

			select {
			case err := <-amqpConnCloseCh:
				select {
				case connCloseCh <- err:
				default:
				}
			case <-publisherCloseCh:
				return
			}
		case <-publisherCloseCh:
			return
		}
	}
}
