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
	dialer, err := amqpextra.NewDialer(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))
	if err != nil {
		log.Fatal(err)
	}

	// create consumer
	c := dialer.Consumer(
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
	<-c.NotifyClosed()

	// close connection
	dialer.Close()

	// Output:
}

func ExampleNewConsumer() {
	// you can get connCh from dialer.ConnectionCh() method
	var connCh chan *amqpextra.Connection

	// create consumer
	c := amqpextra.NewConsumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
		connCh,
	)
	// run consumer
	go c.Run()

	// close consumer
	c.Close()
	<-c.NotifyClosed()

	// Output:
}
