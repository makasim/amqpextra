package examples

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func PublisherExample() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Std)
	p := conn.Publisher()

	resultCh := make(chan error)
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
