package middleware_test

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/mock"
)

type loggerStub struct {
	Formats []string
	Args    [][]interface{}
}

func (l *loggerStub) Info(args ...interface{}) {
	panic("implement me")
}

func (l *loggerStub) Error(args ...interface{}) {
	aS, ok := args[0].(string)
	if ok {
		l.Formats = append(l.Formats, "[ERROR] "+aS)
		l.Args = append(l.Args, args[1:])
	}
}

func (l *loggerStub) Errorf(format string, args ...interface{}) {
	l.Formats = append(l.Formats, "[ERROR] "+format)
	l.Args = append(l.Args, args)
}

func (l *loggerStub) Debug(args ...interface{}) {
	panic("implement me")
}

func (l *loggerStub) Debugf(format string, args ...interface{}) {
	panic("implement me")
}

func (l *loggerStub) Warn(args ...interface{}) {
	aS, ok := args[0].(string)
	if ok {
		l.Formats = append(l.Formats, "[WARN] "+aS)
		l.Args = append(l.Args, args[1:])
	}

}

func (l *loggerStub) Warnf(format string, args ...interface{}) {
	panic("implement me")
}

func (l *loggerStub) Printf(format string, v ...interface{}) {
	l.Formats = append(l.Formats, format)
	l.Args = append(l.Args, v)
}

type acknowledgerMock struct {
	mock.Mock
}

func (m *acknowledgerMock) Ack(tag uint64, multiple bool) error {
	args := m.Called(tag, multiple)

	return args.Error(0)
}

func (m *acknowledgerMock) Nack(tag uint64, multiple, requeue bool) error {
	args := m.Called(tag, multiple, requeue)

	return args.Error(0)
}

func (m *acknowledgerMock) Reject(tag uint64, requeue bool) error {
	args := m.Called(tag, requeue)

	return args.Error(0)
}

func dummyHandler(result interface{}) consumer.Handler {
	return consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return result
	})
}
