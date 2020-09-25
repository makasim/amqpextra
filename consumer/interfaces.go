package consumer

import "github.com/streadway/amqp"

type Connection interface {
	Channel() (Channel, error)
}

type Channel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	Close() error
}

type AMQP struct {
	Conn *amqp.Connection
}

func (c *AMQP) Channel() (Channel, error) {
	return c.Conn.Channel()
}
