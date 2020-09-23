package middleware_test

import (
	"context"
	"testing"

	"strconv"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpireToTimeoutSetContextTimeout(t *testing.T) {
	expiration := strconv.FormatInt((time.Millisecond * 100).Milliseconds(), 10)

	msg := amqp.Delivery{}
	msg.Expiration = expiration

	parentCtx := context.Background()

	worker := middleware.ExpireToTimeout(0)(amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		require.NotEqual(t, parentCtx, ctx)

		select {
		case <-ctx.Done():
			assert.Fail(t, "context must not be done")
		default:
		}

		time.Sleep(110 * time.Millisecond)

		select {
		case <-ctx.Done():
		default:
			assert.Fail(t, "context must be done")
		}

		return nil
	}))

	worker.ServeMsg(parentCtx, msg)
}

func TestExpireToTimeoutNoExpirationNoDefault(t *testing.T) {
	msg := amqp.Delivery{}

	parentCtx := context.Background()

	worker := middleware.ExpireToTimeout(0)(amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		require.Same(t, parentCtx, ctx)

		return nil
	}))

	worker.ServeMsg(parentCtx, msg)
}

func TestExpireToTimeoutNoExpirationSetDefault(t *testing.T) {
	msg := amqp.Delivery{}

	parentCtx := context.Background()

	worker := middleware.ExpireToTimeout(100 * time.Millisecond)(amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		require.NotEqual(t, parentCtx, ctx)

		select {
		case <-ctx.Done():
			assert.Fail(t, "context must not be done")
		default:
		}

		time.Sleep(110 * time.Millisecond)

		select {
		case <-ctx.Done():
		default:
			assert.Fail(t, "context must be done")
		}

		return nil
	}))

	worker.ServeMsg(parentCtx, msg)
}

func TestExpireToTimeoutExpirationInvalidAndNoDefault(t *testing.T) {
	msg := amqp.Delivery{}

	parentCtx := context.Background()

	worker := middleware.ExpireToTimeout(0)(amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		require.Same(t, parentCtx, ctx)

		return nil
	}))

	worker.ServeMsg(parentCtx, msg)
}

func TestExpireToTimeoutExpirationInvalidAndSetDefault(t *testing.T) {
	msg := amqp.Delivery{}
	msg.Expiration = "invalid"

	parentCtx := context.Background()

	worker := middleware.ExpireToTimeout(100 * time.Millisecond)(amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		require.NotEqual(t, parentCtx, ctx)

		select {
		case <-ctx.Done():
			assert.Fail(t, "context must not be done")
		default:
		}

		time.Sleep(110 * time.Millisecond)

		select {
		case <-ctx.Done():
		default:
			assert.Fail(t, "context must be done")
		}

		return nil
	}))

	worker.ServeMsg(parentCtx, msg)
}
