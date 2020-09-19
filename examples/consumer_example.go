package examples

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func ConsumerExample() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Std)

	consumer := conn.Consumer(
		"some_queue",
		amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
	)
	consumer.SetWorkerNum(5)

	consumer.Run()
}
