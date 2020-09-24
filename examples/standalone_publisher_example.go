package examples

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func StandalonePublisherExample() {
	resultCh := make(chan error)
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	p := amqpextra.NewPublisher(
		connCh,
		closeCh,
		publisher.WithLogger(logger.Std),
	)

	p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
		ResultCh: resultCh,
	})

	if err := <-resultCh; err != nil {
		log.Fatal(err)
	}
}
