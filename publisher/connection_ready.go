package publisher

type ConnectionReady interface {
	Conn() AMQPConnection
	NotifyLost() chan struct{}
	NotifyClose() chan struct{}
}

func NewConnectionReady(conn AMQPConnection, lostCh, closeCh chan struct{}) ConnectionReady {
	return &connectionReady{
		conn:        conn,
		notifyLost:  lostCh,
		notifyClose: closeCh,
	}
}

type connectionReady struct {
	conn        AMQPConnection
	notifyLost  chan struct{}
	notifyClose chan struct{}
}

func (cr *connectionReady) Conn() AMQPConnection {
	return cr.conn
}

func (cr *connectionReady) NotifyLost() chan struct{} {
	return cr.notifyClose
}

func (cr *connectionReady) NotifyClose() chan struct{} {
	return cr.notifyClose
}
