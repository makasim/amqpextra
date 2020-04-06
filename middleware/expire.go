package middleware

import (
	"context"

	"strconv"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func ExpireToTimeout(defaultTimeout time.Duration) func(next amqpextra.Worker) amqpextra.Worker {
	return wrap(func(ctx context.Context, msg amqp.Delivery, next amqpextra.Worker) (result interface{}) {
		if msg.Expiration == "" {
			if defaultTimeout.Nanoseconds() == 0 {
				return next.ServeMsg(ctx, msg)
			}

			nextCtx, cancelFunc := context.WithTimeout(ctx, defaultTimeout)
			defer cancelFunc()

			return next.ServeMsg(nextCtx, msg)
		}

		expiration, err := strconv.ParseInt(msg.Expiration, 10, 0)
		if err != nil {
			log(ctx, "[WARN] got invalid expiration: %s", msg.Expiration)

			if defaultTimeout.Nanoseconds() != 0 {
				nextCtx, cancelFunc := context.WithTimeout(ctx, defaultTimeout)
				defer cancelFunc()

				return next.ServeMsg(nextCtx, msg)
			}
		}

		nextCtx, cancelFunc := context.WithTimeout(ctx, time.Duration(expiration)*time.Millisecond)
		defer cancelFunc()

		return next.ServeMsg(nextCtx, msg)
	})
}
