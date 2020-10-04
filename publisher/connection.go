package publisher

func NewConnection(amqpConn AMQPConnection, closeCh chan struct{}) *Connection {
	return &Connection{
		amqpConn:    amqpConn,
		notifyClose: closeCh,
	}
}

type Connection struct {
	amqpConn    AMQPConnection
	notifyClose chan struct{}
}

func (c *Connection) AMQPConnection() AMQPConnection {
	return c.amqpConn
}

func (c *Connection) NotifyClose() chan struct{} {
	return c.notifyClose
}
