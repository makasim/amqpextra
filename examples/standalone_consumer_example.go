package examples

import (
	"context"
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

func StandaloneConsumerExample() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	// usually it equals to pre_fetch_count
	workersNum := 5
	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		// process message

		msg.Ack(false)

		return nil
	})

	consumer := amqpextra.NewConsumer("some_queue", worker, connCh, closeCh)
	consumer.SetLogger(logger.LoggerFunc(log.Printf))
	consumer.SetWorkerNum(workersNum)

	consumer.Run()
}
