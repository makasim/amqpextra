package amqpextra

import (
	"reflect"
)

func NewDialerPool(dialers []*Dialer) <-chan *Connection {
	connCh := make(chan *Connection)

	cases := make([]reflect.SelectCase, len(dialers))
	for i, d := range dialers {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(d.connCh)}
	}

	go func() {
		for {
			if len(cases) == 0 {
				return
			}

			chosen, value, ok := reflect.Select(cases)
			if !ok {
				cases = append(cases[:chosen], cases[chosen+1:]...)
				continue
			}

			conn := value.Interface().(*Connection)

			select {
			case connCh <- conn:
				continue
			case <-conn.NotifyLost():
				continue
			}
		}
	}()

	return connCh
}
