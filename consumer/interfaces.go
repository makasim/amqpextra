package consumer

import "github.com/streadway/amqp"

type ConnectionReady interface {
	Conn() Connection
	NotifyClose() chan struct{}
}

type Connection interface {
}

type Channel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	Close() error
}

func NewConnectionReady(conn Connection) ConnectionReady {
	return &connectionReady{
		conn:        conn,
		notifyClose: make(chan struct{}),
	}
}

type connectionReady struct {
	conn        Connection
	notifyClose chan struct{}
}

func (cr *connectionReady) Conn() Connection {
	return cr.conn
}

func (cr *connectionReady) NotifyClose() chan struct{} {
	return cr.notifyClose
}
