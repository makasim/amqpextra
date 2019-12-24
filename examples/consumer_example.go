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
	ctx := context.Background()

	// usually it equals to pre_fetch_count
	workersNum := 5

	consumer := amqpextra.NewConsumer(
		connCh,
		closeCh,
		ctx,
		amqpextra.LoggerFunc(log.Printf), // or nil
	)

	consumer.Run(workersNum, initMsgCh, amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
		// process message

		msg.Ack(false)

		return nil
	}))
}

func initMsgCh(conn *amqp.Connection) (<-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("amqp: create channel: %s", err)

		return nil, err
	}

	q, err := ch.QueueDeclare("test-queue", true, false, false, false, nil)
	if err != nil {
		log.Printf("amqp: declare queue: %s", err)

		return nil, err
	}

	msgCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("amqp: consume: %s", err)

		return nil, err
	}

	return msgCh, nil
}
