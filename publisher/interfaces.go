package publisher

import "github.com/streadway/amqp"

type Connection interface {
	Channel() (Channel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
}

type AMQP struct {
	Conn *amqp.Connection
}

func (c *AMQP) Channel() (Channel, error) {
	return c.Conn.Channel()
}

func (c *AMQP) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	return c.Conn.NotifyClose(receiver)
}

func (c *AMQP) Close() error {
	return c.Conn.Close()
}

type Channel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
}
