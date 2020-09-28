package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	connCh <-chan Ready,
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
	connCh <-chan Ready,
	consumerConnCh chan consumer.Connection,
	consumerConnCloseCh chan *amqp.Error,
	consumerCloseCh <-chan struct{},
) {
	go func() {
		defer close(consumerConnCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case consumerConnCh <- &consumer.AMQP{Conn: conn.Conn()}:
				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
				return
			}
		}
	}()

	go func() {
		defer close(consumerConnCloseCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case <-conn.NotifyClose():
					select {
					case consumerConnCloseCh <- amqp.ErrClosed:
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
