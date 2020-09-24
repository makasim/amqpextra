package middleware

import (
	"context"

	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/consumer"
)

func HasCorrelationID() func(next consumer.Handler) consumer.Handler {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		if msg.CorrelationId == "" {
			log(ctx, "[WARN] no correlation id")

			return nack(ctx, msg)
		}

		return next.Handle(ctx, msg)
	})
}
