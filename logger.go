package amqpextra

type Logger interface {
	Printf(format string, v ...interface{})
}

type LoggerFunc func(format string, v ...interface{})

func (f LoggerFunc) Printf(format string, v ...interface{}) {
	f(format, v...)
}

func nilLogger() Logger {
	return LoggerFunc(func(format string, v ...interface{}) {})
}
