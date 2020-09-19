package examples

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

func ConsumerExample() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.LoggerFunc(log.Printf))

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
