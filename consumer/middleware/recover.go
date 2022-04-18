package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Recover() consumer.Middleware {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) (result interface{}) {
		defer func() {
			if e := recover(); e != nil {
				log(ctx, "[ERROR] handler panicked: %v", e)

				if nackErr := msg.Nack(false, false); nackErr != nil {
					log(ctx, "[ERROR] msg nack: %v", e)
				}
			}
		}()

		return next.Handle(ctx, msg)
	})
}
