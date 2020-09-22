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

	go func() {
		defer close(connCh)
		defer close(connCloseCh)

		for {
			select {
			case conn, ok := <-amqpConnCh:
				if !ok {
					return
				}

				select {
				case connCh <- &publisher.AMQP{Conn: conn}:
				case <-p.Closed():
					return
				}

				select {
				case err := <-amqpConnCloseCh:
					connCloseCh <- err
				case <-p.Closed():
					return
				}
			case <-p.Closed():
				return
			}
		}
	}()

	return p
}
