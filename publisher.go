package amqpextra

import (
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func NewPublisher(
	connCh <-chan Established,
	opts ...publisher.Option,
) *publisher.Publisher {
	pubConnCh := make(chan publisher.Connection)
	pubConnCloseCh := make(chan *amqp.Error, 1)

	p := publisher.New(pubConnCh, pubConnCloseCh, opts...)
	go proxyPublisherConn(connCh, pubConnCh, pubConnCloseCh, p.Closed())

	return p
}

//nolint:dupl // ignore linter err
func proxyPublisherConn(
	connCh <-chan Established,
	pubConnCh chan publisher.Connection,
	pubConnCloseCh chan *amqp.Error,
	publisherCloseCh <-chan struct{},
) {
	go func() {
		defer close(pubConnCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case pubConnCh <- &publisher.AMQP{Conn: conn.Conn()}:
				case <-publisherCloseCh:
					return
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()

	go func() {
		defer close(pubConnCloseCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				select {
				case <-conn.NotifyClose():
					select {
					case pubConnCloseCh <- amqp.ErrClosed:
					default:
					}
				case <-publisherCloseCh:
					return
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()
}
