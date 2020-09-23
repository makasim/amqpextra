package middleware

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

const Ack = "ack"
const Nack = "nack_requeue"
const Requeue = "requeue"

func AckNack() func(next amqpextra.Worker) amqpextra.Worker {
	return func(next amqpextra.Worker) amqpextra.Worker {
		fn := func(ctx context.Context, msg amqp.Delivery) interface{} {
			result := next.ServeMsg(ctx, msg)
			if result == nil {
				return nil
			}

			l, ok := GetLogger(ctx)
			if !ok {
				l = logger.Discard
			}

			switch result {
			case Ack:
				if err := msg.Ack(false); err != nil {
					l.Printf("[ERROR] message ack errored", err)

					return nil
				}

				l.Printf("[DEBUG] message acked")

				return nil
			case Nack:
				if err := msg.Nack(false, false); err != nil {
					l.Printf("[ERROR] message nack errored", err)

					return nil
				}

				l.Printf("[DEBUG] message nacked")

				return nil
			case Requeue:
				if err := msg.Nack(false, true); err != nil {
					l.Printf("[ERROR] message nack requeue errored", err)

					return nil
				}

				l.Printf("[DEBUG] message requeue")

				return nil
			}

			return result
		}

		return amqpextra.WorkerFunc(fn)
	}
}
