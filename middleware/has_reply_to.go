package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func HasReplyTo() func(next amqpextra.Worker) amqpextra.Worker {
	return func(next amqpextra.Worker) amqpextra.Worker {
		fn := func(ctx context.Context, msg amqp.Delivery) interface{} {
			if msg.ReplyTo == "" {
				log(ctx, "[WARN] no reply to")

				if err := msg.Nack(false, false); err != nil {
					log(ctx, "[ERROR] msg nack: %s", err)
				}

				return nil
			}

			return next.ServeMsg(ctx, msg)
		}

		return amqpextra.WorkerFunc(fn)
	}
}
