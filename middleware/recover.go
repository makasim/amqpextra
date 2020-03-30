package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func Recover() func(next amqpextra.Worker) amqpextra.Worker {
	return func(next amqpextra.Worker) amqpextra.Worker {
		fn := func(ctx context.Context, msg amqp.Delivery) (result interface{}) {
			defer func() {
				if e := recover(); e != nil {
					log(ctx, "[ERROR] worker panicked: %v", e)

					if nackErr := msg.Nack(false, false); nackErr != nil {
						log(ctx, "[ERROR] msg nack: %v", e)
					}
				}
			}()

			return next.ServeMsg(ctx, msg)
		}

		return amqpextra.WorkerFunc(fn)
	}
}
