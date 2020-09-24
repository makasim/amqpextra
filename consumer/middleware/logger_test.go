package middleware_test

import (
	"context"
	"testing"

	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/makasim/amqpextra/consumer"
)

func TestLoggerMiddleware(t *testing.T) {
	l := &loggerStub{}

	handlerFunc := consumer.HandlerFunc(func(ctx context.Context, _ amqp.Delivery) interface{} {
		ll, ok := middleware.GetLogger(ctx)

		require.True(t, ok)
		require.Same(t, l, ll)

		return "theResult"
	})

	ctx := context.Background()

	handler := middleware.Logger(l)(handlerFunc)

	res := handler.Handle(ctx, amqp.Delivery{})
	assert.Equal(t, "theResult", res)
}

func TestLoggerWithLogger(t *testing.T) {
	l := &loggerStub{}

	ctx := context.Background()
	ctx = middleware.WithLogger(ctx, l)

	handler := consumer.HandlerFunc(func(ctx context.Context, _ amqp.Delivery) interface{} {
		ll, ok := middleware.GetLogger(ctx)

		require.True(t, ok)
		require.Same(t, l, ll)

		return "theResult"
	})

	res := handler.Handle(ctx, amqp.Delivery{})
	assert.Equal(t, "theResult", res)
}

func TestLoggerGetLoggerNotSet(t *testing.T) {
	l, ok := middleware.GetLogger(context.Background())

	assert.False(t, ok)
	assert.Nil(t, l)
}

func TestLoggerGetLoggerSet(t *testing.T) {
	l := &loggerStub{}

	ctx := context.Background()
	ctx = middleware.WithLogger(ctx, l)

	ll, ok := middleware.GetLogger(ctx)

	assert.True(t, ok)
	assert.Same(t, l, ll)
}
