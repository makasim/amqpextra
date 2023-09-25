package middleware_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/makasim/amqpextra/consumer/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAckNack(main *testing.T) {
	main.Run("DoNothingIfResultNil", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		msg := amqp.Delivery{}
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		ctx := context.Background()

		handler := middleware.AckNack()(dummyHandler(nil))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)
	})

	main.Run("ResultNack", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Nack", uint64(1234), false, false).Return(nil)

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		ctx := context.Background()

		handler := middleware.AckNack()(dummyHandler(middleware.Nack))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)
	})

	main.Run("NackErrored", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Nack", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("an error"))

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		l := &loggerStub{}

		ctx := context.Background()
		ctx = middleware.WithLogger(ctx, l)

		handler := middleware.AckNack()(dummyHandler(middleware.Nack))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)

		require.Len(t, l.Formats, 1)
		assert.Equal(t, "[ERROR] message nack errored", l.Formats[0])

		require.Len(t, l.Args, 1)
		assert.Equal(t, "[an error]", fmt.Sprintf("%v", l.Args[0]))
	})

	main.Run("ResultRequeue", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Nack", uint64(1234), false, true).Return(nil)

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		ctx := context.Background()

		handler := middleware.AckNack()(dummyHandler(middleware.Requeue))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)
	})

	main.Run("RequeueErrored", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Nack", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("an error"))

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		l := &loggerStub{}

		ctx := context.Background()
		ctx = middleware.WithLogger(ctx, l)

		handler := middleware.AckNack()(dummyHandler(middleware.Requeue))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)

		require.Len(t, l.Formats, 1)
		assert.Equal(t, "[ERROR] message nack requeue errored", l.Formats[0])

		require.Len(t, l.Args, 1)
		assert.Equal(t, "[an error]", fmt.Sprintf("%v", l.Args[0]))
	})

	main.Run("ResultAck", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Ack", uint64(1234), false).Return(nil)

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		ctx := context.Background()

		handler := middleware.AckNack()(dummyHandler(middleware.Ack))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)
	})

	main.Run("AckErrored", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		a.On("Ack", mock.Anything, mock.Anything).Return(fmt.Errorf("an error"))

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		l := &loggerStub{}

		ctx := context.Background()
		ctx = middleware.WithLogger(ctx, l)

		handler := middleware.AckNack()(dummyHandler(middleware.Ack))

		res := handler.Handle(ctx, msg)
		assert.Nil(t, res)

		require.Len(t, l.Formats, 1)
		assert.Equal(t, "[ERROR] message ack errored", l.Formats[0])

		require.Len(t, l.Args, 1)
		assert.Equal(t, "[an error]", fmt.Sprintf("%v", l.Args[0]))
	})

	main.Run("UnrecognizedResult", func(t *testing.T) {
		a := acknowledgerMock{}
		defer a.AssertExpectations(t)

		msg := amqp.Delivery{}
		msg.DeliveryTag = 1234
		msg.CorrelationId = "theID"
		msg.Acknowledger = &a

		expected := struct{}{}

		ctx := context.Background()

		handler := middleware.AckNack()(dummyHandler(expected))

		res := handler.Handle(ctx, msg)
		assert.Equal(t, expected, res)
	})
}
