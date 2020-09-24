package publisher

import "github.com/streadway/amqp"

type Connection interface {
	Channel() (Channel, error)
}

type Channel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyFlow(c chan bool) chan bool
	Close() error
}

type AMQP struct {
	Conn *amqp.Connection
}

func (c *AMQP) Channel() (Channel, error) {
	return c.Conn.Channel()
}
