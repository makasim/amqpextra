package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	connCh <-chan *Connection,
	opts ...consumer.Option,
) (*consumer.Consumer, error) {
	consumerConnCh := make(chan *consumer.Connection)

	c, err := consumer.New(queue, handler, consumerConnCh, opts...)
	if err != nil {
		return nil, err
	}

	go proxyConsumerConn(connCh, consumerConnCh, c.NotifyClosed())

	return c, nil
}

//nolint:dupl // ignore linter err
func proxyConsumerConn(
	connCh <-chan *Connection,
	consumerConnCh chan *consumer.Connection,
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
