package publisher

import "github.com/streadway/amqp"

func NewBridge(
	amqpConnCh <-chan *amqp.Connection,
	amqpConnCloseCh <-chan *amqp.Error,
	opts ...Option,
) *Publisher {
	connCh := make(chan Connection)
	connCloseCh := make(chan *amqp.Error, 1)

	p := New(connCh, connCloseCh, opts...)

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
				case connCh <- &AMQP{Conn: conn}:
				case <-p.closeCh:
					return
				}

				select {
				case err := <-amqpConnCloseCh:
					connCloseCh <- err
				case <-p.closeCh:
					return
				}
			case <-p.closeCh:
				return
			}
		}
	}()

	return p
}
