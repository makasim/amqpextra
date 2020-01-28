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

	publisher.Publish(amqpextra.Publishing{
		Key:       "test_queue",
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
		ResultCh: resultCh,
	})

	if err := <-resultCh; err != nil {
		log.Fatal(err)
	}
}
