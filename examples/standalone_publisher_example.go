package examples

import (
	"log"

	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

func StandalonePublisherExample() {
	resultCh := make(chan error)
	connCh := make(<-chan *amqp.Connection)
	closeCh := make(<-chan *amqp.Error)

	p := publisher.New(
		connCh,
		closeCh,
		publisher.WithLogger(logger.LoggerFunc(log.Printf)),
	)

	p.Publish(publisher.Message{
		Key:       "test_queue",
		WaitReady: true,
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
		ResultCh: resultCh,
	})

	if err := <-resultCh; err != nil {
		log.Fatal(err)
	}
}
