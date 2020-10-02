package publisher

type ConnectionReady interface {
	Conn() Connection
	NotifyClose() chan struct{}
}

func NewConnectionReady(conn Connection, closeCh chan struct{}) ConnectionReady {
	return &connectionReady{
		conn:        conn,
		notifyClose: closeCh,
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
