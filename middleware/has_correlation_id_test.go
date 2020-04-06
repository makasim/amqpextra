package middleware_test

import (
	"context"
	"testing"

	"fmt"

	"github.com/makasim/amqpextra/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasCorrelationIDSet(t *testing.T) {
	a := acknowledgerMock{}
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.CorrelationId = "theID"
	msg.Acknowledger = &a

	ctx := context.Background()

	worker := middleware.HasCorrelationID()(dummyWorker(nil))

	res := worker.ServeMsg(ctx, msg)
	assert.Nil(t, res)
}

func TestHasCorrelationIDNotSet(t *testing.T) {
	deliveryTag := uint64(1234)

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(nil)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a
	ctx := context.Background()

	worker := middleware.HasCorrelationID()(dummyWorker(nil))

	res := worker.ServeMsg(ctx, msg)
	assert.Nil(t, res)
}

func TestHasCorrelationIDNotSetWithLogger(t *testing.T) {
	deliveryTag := uint64(1234)

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(nil)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a

	l := &loggerStub{}

	ctx := context.Background()
	ctx = middleware.WithLogger(ctx, l)

	worker := middleware.HasCorrelationID()(dummyWorker(nil))

	res := worker.ServeMsg(ctx, msg)
	assert.Nil(t, res)

	require.Len(t, l.Formats, 1)
	assert.Equal(t, "[WARN] no correlation id", l.Formats[0])
}

func TestHasCorrelationIDNotSetAndNackErrored(t *testing.T) {
	deliveryTag := uint64(1234)

	err := fmt.Errorf("nack errored")

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(err)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a

	ctx := context.Background()

	worker := middleware.HasCorrelationID()(dummyWorker(nil))

	res := worker.ServeMsg(ctx, msg)
	assert.Nil(t, res)
}

func TestHasCorrelationIDNotSetAndNackErroredWithLogger(t *testing.T) {
	deliveryTag := uint64(1234)

	err := fmt.Errorf("nack errored")

	a := acknowledgerMock{}
	a.On("Nack", deliveryTag, false, false).Return(err)
	defer a.AssertExpectations(t)

	msg := amqp.Delivery{}
	msg.DeliveryTag = deliveryTag
	msg.Acknowledger = &a

	l := &loggerStub{}

	ctx := context.Background()
	ctx = middleware.WithLogger(ctx, l)

	worker := middleware.HasCorrelationID()(dummyWorker(nil))

	res := worker.ServeMsg(ctx, msg)
	assert.Nil(t, res)

	require.Len(t, l.Formats, 2)
	assert.Equal(t, "[WARN] no correlation id", l.Formats[0])

	assert.Equal(t, "[ERROR] msg nack: %s", l.Formats[1])
	require.Len(t, l.Args[1], 1)
	assert.EqualError(t, l.Args[1][0].(error), "nack errored")
}
