package main

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(amqpextra.LoggerFunc(log.Printf))
	publisher := conn.Publisher()

	resultCh := make(chan error)
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
