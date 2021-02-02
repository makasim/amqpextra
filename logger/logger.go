package logger

var Discard = Func(func(format string, v ...interface{}) {})

type Logger interface {
	Printf(format string, args ...interface{})
	Info(args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
}

type Func func(format string, args ...interface{})

func (f Func) Info(args ...interface{}) {
	f("", args...)
}

func (f Func) Error(args ...interface{}) {
	f("", args...)
}

func (f Func) Errorf(format string, args ...interface{}) {
	f(format, args...)
}

func (f Func) Debug(args ...interface{}) {
	f("", args...)
}

func (f Func) Debugf(format string, args ...interface{}) {
	f(format, args...)
}

func (f Func) Warn(args ...interface{}) {
	f("", args...)
}

func (f Func) Warnf(format string, args ...interface{}) {
	f(format, args...)
}

func (f Func) Printf(format string, args ...interface{}) {
	f(format, args...)
}
