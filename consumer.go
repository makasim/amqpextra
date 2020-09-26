package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	connCh <-chan Connection,
	opts ...consumer.Option,
) *consumer.Consumer {
	consConnCh := make(chan consumer.Connection)
	consConnCloseCh := make(chan *amqp.Error, 1)

	c := consumer.New(queue, handler, consConnCh, consConnCloseCh, opts...)
	go proxyConsumerConn(connCh, consConnCh, consConnCloseCh, c.Closed())

	return c
}

//nolint:dupl // ignore linter err
func proxyConsumerConn(
	connCh <-chan Connection,
	consConnCh chan consumer.Connection,
	consConnCloseCh chan *amqp.Error,
	consumerCloseCh <-chan struct{},
) {
	go func() {
		defer close(consConnCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case consConnCh <- &consumer.AMQP{Conn: conn.AMQPConnection()}:
				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
				return
			}
		}
	}()

	go func() {
		defer close(consConnCloseCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case err, ok := <-conn.NotifyClose():
					if !ok {
						err = amqp.ErrClosed
					}

					select {
					case consConnCloseCh <- err:
					default:
					}
				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
				return
			}
		}
	}()
}
