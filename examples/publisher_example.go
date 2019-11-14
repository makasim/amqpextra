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

	publisher := amqpextra.NewPublisher(
		connCh,
		closeCh,
		ctx.Done(),
		intiCh,
		log.Printf,
		log.Printf,
	)

	err := <-publisher.Publish(
		"",
		"test_queue",
		false,
		false,
		amqp.Publishing{
		Body:        []byte(`{"foo": "fooVal"}`),
	})
	if err != nil {
		panic(err)
	}
}

func intiCh(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		"test_queue",
		true,
		false,
		false,
		true,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return ch, nil
}
