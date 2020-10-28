package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func HasCorrelationID() consumer.Middleware {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		if msg.CorrelationId == "" {
			log(ctx, "[WARN] no correlation id")

			return nack(ctx, msg)
		}

		return next.Handle(ctx, msg)
	})
}
