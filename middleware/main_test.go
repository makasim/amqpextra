package middleware_test

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/mock"
)

type loggerStub struct {
	Formats []string
	Args    [][]interface{}
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

func dummyWorker(result interface{}) amqpextra.Worker {
	return amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return result
	})
}
