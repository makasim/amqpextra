package middleware

import (
	"context"

	"strconv"

	"time"

	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func ExpireToTimeout(defaultTimeout time.Duration) func(next consumer.Handler) consumer.Handler {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next consumer.Handler) (result interface{}) {
		if msg.Expiration == "" {
			if defaultTimeout.Nanoseconds() == 0 {
				return next.Handle(ctx, msg)
			}

			nextCtx, cancelFunc := context.WithTimeout(ctx, defaultTimeout)
			defer cancelFunc()

			return next.Handle(nextCtx, msg)
		}

		expiration, err := strconv.ParseInt(msg.Expiration, 10, 0)
		if err != nil {
			log(ctx, "[WARN] got invalid expiration: %s", msg.Expiration)

			if defaultTimeout.Nanoseconds() != 0 {
				nextCtx, cancelFunc := context.WithTimeout(ctx, defaultTimeout)
				defer cancelFunc()

				return next.Handle(nextCtx, msg)
			}
		}

		nextCtx, cancelFunc := context.WithTimeout(ctx, time.Duration(expiration)*time.Millisecond)
		defer cancelFunc()

		return next.Handle(nextCtx, msg)
	})
}
