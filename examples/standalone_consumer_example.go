package examples

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func StandaloneConsumerExample() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	handler := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		// process message

		msg.Ack(false)

		return nil
	})

	c := amqpextra.NewConsumer(
		"some_queue",
		handler,
		connCh,
		closeCh,
		consumer.WithLogger(logger.Std),
	)

	c.Run()
}
