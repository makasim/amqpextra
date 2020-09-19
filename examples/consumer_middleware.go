package examples

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

func ConsumerMiddleware() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	consumer := amqpextra.NewConsumer(
		"a_queue",
		amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		connCh,
		closeCh,
	)

	consumer.SetLogger(logger.LoggerFunc(log.Printf))

	consumer.Use(func(next amqpextra.Worker) amqpextra.Worker {
		fn := func(ctx context.Context, msg amqp.Delivery) interface{} {
			if msg.CorrelationId == "" {
				msg.Nack(true, true)

				return nil
			}
			if msg.ReplyTo == "" {
				msg.Nack(true, true)

				return nil
			}

			return next.ServeMsg(ctx, msg)
		}

		return amqpextra.WorkerFunc(fn)
	})

	consumer.Run()
}
