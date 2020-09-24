package middleware

import (
	"context"

	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/consumer"
)

func HasReplyTo() func(next consumer.Handler) consumer.Handler {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) interface{} {
		if msg.ReplyTo == "" {
			log(ctx, "[WARN] no reply to")

			return nack(ctx, msg)
		}

		return next.Handle(ctx, msg)
	})
}
