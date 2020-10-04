package publisher

type ConnectionReady interface {
	Conn() AMQPConnection
	NotifyClose() chan struct{}
}

func NewConnection(conn AMQPConnection, closeCh chan struct{}) ConnectionReady {
	return &connectionReady{
		conn:        conn,
		notifyClose: closeCh,
	}
}

type connectionReady struct {
	conn        AMQPConnection
	notifyClose chan struct{}
}

func (cr *connectionReady) Conn() AMQPConnection {
	return cr.conn
}

func (cr *connectionReady) NotifyClose() chan struct{} {
	return cr.notifyClose
}
