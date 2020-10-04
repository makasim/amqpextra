package consumer

func NewConnection(amqpConn AMQPConnection, closeCh chan struct{}) *Connection {
	return &Connection{
		amqpConn: amqpConn,
		closeCh:  closeCh,
	}
}

type Connection struct {
	amqpConn AMQPConnection
	closeCh  chan struct{}
}

func (c *Connection) AMQPConnection() AMQPConnection {
	return c.amqpConn
}

func (c *Connection) NotifyClose() chan struct{} {
	return c.closeCh
}
