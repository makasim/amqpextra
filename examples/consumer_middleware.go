package main

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	consumer := amqpextra.NewConsumer(
		"a_queue",
		amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		connCh,
		closeCh,
	)

	consumer.SetLogger(amqpextra.LoggerFunc(log.Printf))

	consumer.Use(func(next amqpextra.Worker) amqpextra.Worker {
		fn := func(msg amqp.Delivery, ctx context.Context) interface{} {
			if msg.CorrelationId == "" {
				msg.Nack(true, true)

				return nil
			}
			if msg.ReplyTo == "" {
				msg.Nack(true, true)

				return nil
			}

			return next.ServeMsg(msg, ctx)
		}

		return amqpextra.WorkerFunc(fn)
	})

	consumer.Run()
}
