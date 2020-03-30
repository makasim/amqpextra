package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

var loggerKey = &contextKey{"logger"}

func Logger(l amqpextra.Logger) func(next amqpextra.Worker) amqpextra.Worker {
	return func(next amqpextra.Worker) amqpextra.Worker {
		return amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			return next.ServeMsg(WithLogger(ctx, l), msg)
		})
	}
}

func WithLogger(ctx context.Context, l amqpextra.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func GetLogger(ctx context.Context) (amqpextra.Logger, bool) {
	l, ok := ctx.Value(loggerKey).(amqpextra.Logger)

	return l, ok
}
