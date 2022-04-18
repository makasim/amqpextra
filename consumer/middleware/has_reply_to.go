package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	amqp "github.com/rabbitmq/amqp091-go"
)

func HasReplyTo() consumer.Middleware {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		if msg.ReplyTo == "" {
			log(ctx, "[WARN] no reply to")

			return nack(ctx, msg)
		}

		return next.Handle(ctx, msg)
	})
}
