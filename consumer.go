package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	connCh <-chan *Connection,
	opts ...consumer.Option,
) *consumer.Consumer {
	consConnCh := make(chan consumer.ConnectionReady)

	c := consumer.New(queue, handler, consConnCh, opts...)
	go proxyConsumerConn(connCh, consConnCh, c.NotifyClosed())

	return c
}

//nolint:dupl // ignore linter err
func proxyConsumerConn(
	connCh <-chan *Connection,
	consumerConnCh chan consumer.ConnectionReady,
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

				consumerConn := consumer.NewConnection(conn.AMQPConnection(), conn.NotifyClose())

				select {
				case consumerConnCh <- consumerConn:
				case <-conn.NotifyClose():
					continue
				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
				return
			}
		}
	}()
}
