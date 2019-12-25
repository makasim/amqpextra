package main

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	resultCh := make(chan error)
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	publisher := amqpextra.NewPublisher(
		connCh,
		closeCh,
	)
	publisher.SetLogger(amqpextra.LoggerFunc(log.Printf))

	publisher.Publish(
		"",
		"test_queue",
		false,
		false,
		amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		}, resultCh)

	if err := <-resultCh; err != nil {
		log.Fatal(err)
	}
}
