package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

const Ack = "ack"
const Nack = "nack_requeue"
const Requeue = "requeue"

func AckNack() consumer.Middleware {
	return func(next consumer.Handler) consumer.Handler {
		fn := func(ctx context.Context, msg amqp.Delivery) interface{} {
			result := next.Handle(ctx, msg)
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

		return consumer.HandlerFunc(fn)
	}
}
