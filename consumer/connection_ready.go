package consumer

type ConnectionReady interface {
	Conn() Connection
	NotifyClose() chan struct{}
}

func NewConnectionReady(conn Connection, closeCh chan struct{}) ConnectionReady {
	return &connectionReady{
		conn:    conn,
		closeCh: closeCh,
	}
}

type connectionReady struct {
	conn    Connection
	closeCh chan struct{}
}

func (cr *connectionReady) Conn() Connection {
	return cr.conn
}

func (cr *connectionReady) NotifyClose() chan struct{} {
	return cr.closeCh
}
