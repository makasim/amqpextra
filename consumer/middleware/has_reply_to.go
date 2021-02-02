package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func HasReplyTo() consumer.Middleware {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		if msg.ReplyTo == "" {
			warn(ctx, "no reply to")

			return nack(ctx, msg)
		}

		return next.Handle(ctx, msg)
	})
}
