package middleware_test

import (
	"context"
	"testing"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverPropagateResultIfNoPanic(t *testing.T) {
	msg := amqp.Delivery{}
	msg.Body = []byte("original-message")
	ctx := context.Background()

	handler := middleware.Recover()(dummyHandler("theResult"))

	reply := handler.Handle(ctx, msg)

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

	handlerFunc := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		panic("a panic")
	})

	handler := middleware.Recover()(handlerFunc)

	reply := handler.Handle(ctx, msg)
	assert.Nil(t, reply)
}

func TestRecoverNoPanicNoLog(t *testing.T) {
	l := &loggerStub{}

	msg := amqp.Delivery{}
	msg.Body = []byte("original-message")
	ctx := middleware.WithLogger(context.Background(), l)

	handler := middleware.Recover()(dummyHandler("aResult"))
	handler.Handle(ctx, msg)

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

	handlerFunc := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		panic("a panic")
	})

	handler := middleware.Recover()(handlerFunc)

	reply := handler.Handle(ctx, msg)
	assert.Nil(t, reply)

	require.Equal(t, 1, len(l.Formats))
	assert.Equal(t, "[ERROR] handler panicked: %v", l.Formats[0])
	assert.Equal(t, 1, len(l.Args[0]))
	assert.Equal(t, "a panic", l.Args[0][0])
}
