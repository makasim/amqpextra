package amqpextra_test

import (
	"context"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func ExampleConnection_Consumer() {
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
	// you can get connCh and closeCh from conn.ConnCh() method
	var connCh chan *amqp.Connection
	var closeCh chan *amqp.Error

	// create consumer
	c := amqpextra.NewConsumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		connCh,
		closeCh,
	)
	// run consumer
	go c.Run()

	// close consumer
	c.Close()
	<-c.Closed()

	// Output:
}
