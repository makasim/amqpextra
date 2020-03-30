package middleware_test

import (
	"context"
	"testing"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverPropagateResultIfNoPanic(t *testing.T) {
	msg := amqp.Delivery{}
	msg.Body = []byte("original-message")
	ctx := context.Background()

	worker := middleware.Recover()(dummyWorker("theResult"))

	reply := worker.ServeMsg(ctx, msg)

	assert.Equal(t, "theResult", reply)
}

func TestRecoverNackOnPanic(t *testing.T) {
	deliveryTag := uint64(1234)

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(nil)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a
	ctx := context.Background()

	workerFunc := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		panic("a panic")
	})

	worker := middleware.Recover()(workerFunc)

	reply := worker.ServeMsg(ctx, msg)
	assert.Nil(t, reply)
}

func TestRecoverNoPanicNoLog(t *testing.T) {
	l := &loggerStub{}

	msg := amqp.Delivery{}
	msg.Body = []byte("original-message")
	ctx := middleware.WithLogger(context.Background(), l)

	worker := middleware.Recover()(dummyWorker("aResult"))
	worker.ServeMsg(ctx, msg)

	assert.Equal(t, 0, len(l.Formats))
}

func TestRecoverLogOnPanic(t *testing.T) {
	l := &loggerStub{}

	deliveryTag := uint64(1234)

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(nil)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a
	ctx := middleware.WithLogger(context.Background(), l)

	workerFunc := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		panic("a panic")
	})

	worker := middleware.Recover()(workerFunc)

	reply := worker.ServeMsg(ctx, msg)
	assert.Nil(t, reply)

	require.Equal(t, 1, len(l.Formats))
	assert.Equal(t, "[ERROR] worker panicked: %v", l.Formats[0])
	assert.Equal(t, 1, len(l.Args[0]))
	assert.Equal(t, "a panic", l.Args[0][0])
}
