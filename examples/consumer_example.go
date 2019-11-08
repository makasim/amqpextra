package main

import (
	"context"
	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)
	ctx := context.TODO()

	// usually it equals to pre_fetch_count
	workersNum := 5

	consumer := amqpextra.NewConsumer(
		connCh,
		closeCh,
		ctx.Done(),
		log.Printf,
		log.Printf,
	)
	consumer.Run(workersNum, initMsgCh, func(msg amqp.Delivery) {
		// process message

		msg.Ack(false)
	})
}

func initMsgCh(conn *amqp.Connection) (<-chan amqp.Delivery, error){
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
