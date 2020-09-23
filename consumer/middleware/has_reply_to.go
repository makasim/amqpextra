package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func HasReplyTo() func(next amqpextra.Worker) amqpextra.Worker {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next amqpextra.Worker) interface{} {
		if msg.ReplyTo == "" {
			log(ctx, "[WARN] no reply to")

			return nack(ctx, msg)
		}

		return next.ServeMsg(ctx, msg)
	})
}
