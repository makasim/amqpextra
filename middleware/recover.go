package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func Recover() func(next amqpextra.Worker) amqpextra.Worker {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next amqpextra.Worker) (result interface{}) {
		defer func() {
			if e := recover(); e != nil {
				log(ctx, "[ERROR] worker panicked: %v", e)

				if nackErr := msg.Nack(false, false); nackErr != nil {
					log(ctx, "[ERROR] msg nack: %v", e)
				}
			}
		}()

		return next.ServeMsg(ctx, msg)
	})
}
