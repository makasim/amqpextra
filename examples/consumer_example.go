package main

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	// usually it equals to pre_fetch_count
	connextra := amqpextra.New(amqpextra.NewDialer("amqp://guest:guest@localhost:5672/%2f", amqp.Config{}))
	connextra.SetLogger(amqpextra.LoggerFunc(log.Printf))

	consumer := connextra.Consumer(
		"some_queue",
		amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
	)
	consumer.SetWorkerNum(5)

	consumer.Run()
}
