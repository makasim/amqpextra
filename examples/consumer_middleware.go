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
	ctx := context.TODO()

	// usually it equals to pre_fetch_count
	workersNum := 5
	worker := amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
		// process message

		msg.Ack(false)

		return nil
	})

	consumer := amqpextra.NewConsumer(
		connCh,
		closeCh,
		ctx,
		log.Printf,
		log.Printf,
	)
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
	consumer.Run(workersNum, initMsgCh, worker)
}
