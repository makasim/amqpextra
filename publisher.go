package amqpextra

import (
	"github.com/makasim/amqpextra/publisher"
)

func NewPublisher(
	connCh <-chan Ready,
	opts ...publisher.Option,
) *publisher.Publisher {
	pubConnCh := make(chan publisher.ConnectionReady)

	p := publisher.New(pubConnCh, opts...)
	go proxyPublisherConn(connCh, pubConnCh, p.Closed())

	return p
}

//nolint:dupl // ignore linter err
func proxyPublisherConn(
	connCh <-chan Ready,
	publisherConnCh chan publisher.ConnectionReady,
	publisherCloseCh <-chan struct{},
) {
	go func() {
		defer close(publisherConnCh)

		for {
			select {
			case connReady, ok := <-connCh:
				if !ok {
					return
				}

				publisherConnReady := publisher.NewConnectionReady(connReady.Conn())

				select {
				case publisherConnCh <- publisherConnReady:
				case <-connReady.NotifyClose():

				case <-publisherCloseCh:
					return
				}
			case <-publisherCloseCh:
				return
			}
		}
	}()
}
