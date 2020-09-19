package examples

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
	"github.com/makasim/amqpextra/logger"
)

func PublisherExample() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.LoggerFunc(log.Printf))
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
