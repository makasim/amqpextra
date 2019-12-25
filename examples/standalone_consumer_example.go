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

	// usually it equals to pre_fetch_count
	workersNum := 5
	worker := amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
		// process message

		msg.Ack(false)

		return nil
	})

	consumer := amqpextra.NewConsumer("some_queue", worker, connCh, closeCh)
	consumer.SetLogger(amqpextra.LoggerFunc(log.Printf))
	consumer.SetWorkerNum(workersNum)

	consumer.Run()
}
