package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

var loggerKey = &contextKey{"logger"}

func Logger(l logger.Logger) func(next consumer.Handler) consumer.Handler {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		return next.Handle(WithLogger(ctx, l), msg)
	})
}

func WithLogger(ctx context.Context, l logger.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func GetLogger(ctx context.Context) (logger.Logger, bool) {
	l, ok := ctx.Value(loggerKey).(logger.Logger)

	return l, ok
}
