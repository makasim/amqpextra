package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

var loggerKey = &contextKey{"logger"}

func Logger(l logger.Logger) func(next amqpextra.Worker) amqpextra.Worker {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next amqpextra.Worker) interface{} {
		return next.ServeMsg(WithLogger(ctx, l), msg)
	})
}

func WithLogger(ctx context.Context, l logger.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func GetLogger(ctx context.Context) (logger.Logger, bool) {
	l, ok := ctx.Value(loggerKey).(logger.Logger)

	return l, ok
}
