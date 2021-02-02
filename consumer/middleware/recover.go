package middleware

import (
	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func Recover() consumer.Middleware {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) (result interface{}) {
		defer func() {
			if e := recover(); e != nil {
				errorf(ctx, "handler panicked: %v", e)

				if nackErr := msg.Nack(false, false); nackErr != nil {
					errorf(ctx, "msg nack: %v", e)
				}
			}
		}()

		return next.Handle(ctx, msg)
	})
}
