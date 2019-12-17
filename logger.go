package amqpextra

import "sync"

type logger struct {
	mx        sync.Mutex
	errorFunc func(format string, v ...interface{})
	debugFunc func(format string, v ...interface{})
}

func (l *logger) SetDebugFunc(f func(format string, v ...interface{})) {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.debugFunc = f
}

func (l *logger) SetErrorFunc(f func(format string, v ...interface{})) {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.errorFunc = f
}

func (l *logger) DebugFunc() func(format string, v ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	return l.debugFunc
}

func (l *logger) ErrorFunc() func(format string, v ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	return l.errorFunc
}

func (l *logger) Debugf(format string, v ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.debugFunc != nil {
		l.debugFunc(format, v...)
	}
}

func (l *logger) Errorf(format string, v ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.errorFunc != nil {
		l.errorFunc(format, v...)
	}
}
