//nolint:dupl // ignore linter err
package amqpextra

import (
	"github.com/makasim/amqpextra/publisher"
)

func NewPublisher(
	connCh <-chan *Connection,
	opts ...publisher.Option,
) (*publisher.Publisher, error) {
	pubConnCh := make(chan *publisher.Connection)

	p, err := publisher.New(pubConnCh, opts...)
	if err != nil {
		return nil, err
	}

	go proxyPublisherConn(connCh, pubConnCh, p.NotifyClosed())

	return p, nil
}

//nolint:dupl // ignore linter err
func proxyPublisherConn(
	connCh <-chan *Connection,
	publisherConnCh chan *publisher.Connection,
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
				case <-conn.NotifyLost():
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
