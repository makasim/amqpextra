package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func HasCorrelationID() func(next amqpextra.Worker) amqpextra.Worker {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next amqpextra.Worker) interface{} {
		if msg.CorrelationId == "" {
			log(ctx, "[WARN] no correlation id")

			return nack(ctx, msg)
		}

		return next.ServeMsg(ctx, msg)
	})
}
