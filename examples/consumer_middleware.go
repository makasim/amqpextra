package examples

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func ConsumerMiddleware() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	handler := consumer.Wrap(
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		middleware.HasCorrelationID(),
		middleware.HasReplyTo(),
	)

	c := amqpextra.NewConsumer(
		"a_queue",
		handler,
		connCh,
		closeCh,
		consumer.WithLogger(logger.Std),
	)

	c.Run()
}
