package amqpextra_test

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func ExampleConnector_Consumer() {
	// open connection
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Discard)

	// create consumer
	c := conn.Consumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
	)
	// run consumer
	go c.Run()

	// close consumer
	c.Close()
	<-c.Closed()

	// close connection
	conn.Close()

	// Output:
}

func ExampleNewConsumer() {
	// you can get estCh (established connections channel) conn.Ready() method
	var estCh chan amqpextra.Established

	// create consumer
	c := amqpextra.NewConsumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		estCh,
	)
	// run consumer
	go c.Run()

	// close consumer
	c.Close()
	<-c.Closed()

	// Output:
}
