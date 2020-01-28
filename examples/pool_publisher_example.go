package main

import (
	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func main() {
	conn1 := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq1:5672/%2f"})
	conn2 := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq2:5672/%2f"})
	p := amqpextra.NewPoolPublisher([]*amqpextra.Publisher{
		conn1.Publisher(),
		conn2.Publisher()},
	)

	p.Publish(amqpextra.Publishing{
		Key:       "test_queue",
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})
}
