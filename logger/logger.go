package logger

import "log"

var Discard = Func(func(format string, v ...interface{}) {})
var Std = Func(log.Printf)

type Logger interface {
	Printf(format string, v ...interface{})
}

type Func func(format string, v ...interface{})

func (f Func) Printf(format string, v ...interface{}) {
	f(format, v...)
}
