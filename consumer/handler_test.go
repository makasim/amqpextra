package consumer_test

import (
	"context"
	"testing"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleWrap() {
	// you can get connCh and closeCh from conn.ConnCh() method
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	// wrap a handler by some middlewares
	handler := consumer.Wrap(
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		middleware.HasCorrelationID(),
		middleware.HasReplyTo(),
	)

	// pass wrapped handler to a consumer
	c := amqpextra.NewConsumer(
		"a_queue",
		handler,
		connCh,
		closeCh,
	)

	go c.Run()

	// close consumer
	c.Close()
	<-c.Closed()

	// Output:
}

func TestWrap(t *testing.T) {
	l := logger.NewTest()

	expectedCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	expectedMsg := amqp.Delivery{Body: []byte(`theBody`)}

	expectedResult := "theResult"

	handler := consumer.Wrap(
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			assert.Equal(t, expectedCtx, ctx)
			assert.Equal(t, expectedMsg, msg)

			l.Printf("[TEST] handler")
			return expectedResult
		}),
		func(next consumer.Handler) consumer.Handler {
			return consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
				assert.Equal(t, expectedCtx, ctx)
				assert.Equal(t, expectedMsg, msg)

				l.Printf("[TEST] handler1 before")
				result := next.Handle(ctx, msg)
				assert.Equal(t, expectedResult, result)
				l.Printf("[TEST] handler1 after")
				return result
			})
		},
		func(next consumer.Handler) consumer.Handler {
			return consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
				assert.Equal(t, expectedCtx, ctx)
				assert.Equal(t, expectedMsg, msg)

				l.Printf("[TEST] handler2 before")
				result := next.Handle(ctx, msg)
				assert.Equal(t, expectedResult, result)
				l.Printf("[TEST] handler2 after")
				return result
			})
		},
	)

	result := handler.Handle(expectedCtx, expectedMsg)
	assert.Equal(t, expectedResult, result)
	require.Equal(t, `[TEST] handler1 before
[TEST] handler2 before
[TEST] handler
[TEST] handler2 after
[TEST] handler1 after
`, l.Logs())
}
