package amqpextra

import (
	"github.com/makasim/amqpextra/consumer"
)

func NewConsumer(
	queue string,
	handler consumer.Handler,
	connCh <-chan Ready,
	opts ...consumer.Option,
) *consumer.Consumer {
	consConnCh := make(chan consumer.ConnectionReady)

	c := consumer.New(queue, handler, consConnCh, opts...)
	go proxyConsumerConn(connCh, consConnCh, c.Closed())

	return c
}

//nolint:dupl // ignore linter err
func proxyConsumerConn(
	connCh <-chan Ready,
	consumerConnCh chan consumer.ConnectionReady,
	consumerCloseCh <-chan struct{},
) {
	go func() {
		defer close(consumerConnCh)

		for {
			select {
			case connReady, ok := <-connCh:
				if !ok {
					return
				}

				consumerConnReady := consumer.NewConnectionReady(connReady.Conn())

				select {
				case consumerConnCh <- consumerConnReady:
				case <-connReady.NotifyClose():

				case <-consumerCloseCh:
					return
				}
			case <-consumerCloseCh:
				return
			}
		}
	}()
}
