package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	opts ...consumer.Option,
) *consumer.Consumer {
	connCh := make(chan consumer.Connection)
	connCloseCh := make(chan *amqp.Error, 1)

	c := consumer.New(queue, handler, connCh, connCloseCh, opts...)
	go proxyConsumerConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, c.Closed())

	return c
}

//nolint:dupl // ignore linter err
func proxyConsumerConn(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	connCh chan consumer.Connection,
	connCloseCh chan *amqp.Error,
	consumerCloseCh <-chan struct{},
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
				case connCh <- &consumer.AMQP{Conn: conn}:
				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
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
			case <-consumerCloseCh:
				return
			}
		}
	}()
}
