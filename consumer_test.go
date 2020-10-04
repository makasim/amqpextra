package amqpextra_test

import (
	"context"

	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/streadway/amqp"
)

func ExampleDialer_Consumer() {
	// open connection
	dialer, _ := amqpextra.NewDialer(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		// process message
		msg.Ack(false)

		return nil
	})

	c, _ := dialer.Consumer("a_queue", h)

	// close consumer
	c.Close()
	<-c.NotifyClosed()

	// close connection
	dialer.Close()

	// Output:
}

func ExampleNewConsumer() {
	// you can get connCh from dialer.ConnectionCh() method
	var connCh chan *amqpextra.Connection

	// create consumer
	c, err := amqpextra.NewConsumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		connCh,
	)
	if err != nil {
		log.Fatal(err)
	}

	// close consumer
	c.Close()
	<-c.NotifyClosed()

	// Output:
}
