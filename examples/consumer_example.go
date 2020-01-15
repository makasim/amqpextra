package main

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(amqpextra.LoggerFunc(log.Printf))

	consumer := conn.Consumer(
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
