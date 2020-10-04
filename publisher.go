package amqpextra

import (
	"github.com/makasim/amqpextra/publisher"
)

func NewPublisher(
	connCh <-chan *Connection,
	opts ...publisher.Option,
) *publisher.Publisher {
	pubConnCh := make(chan publisher.ConnectionReady)

	p := publisher.New(pubConnCh, opts...)
	go proxyPublisherConn(connCh, pubConnCh, p.NotifyClosed())

	return p
}

//nolint:dupl // ignore linter err
func proxyPublisherConn(
	connCh <-chan *Connection,
	publisherConnCh chan publisher.ConnectionReady,
	publisherCloseCh <-chan struct{},
) {
	go func() {
		defer close(publisherConnCh)

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					return
				}

				publisherConn := publisher.NewConnection(
					conn.AMQPConnection(),
					conn.NotifyLost(),
				)

				select {
				case publisherConnCh <- publisherConn:
				case <-conn.NotifyClose():
					continue
				case <-publisherCloseCh:
					return
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()
}
