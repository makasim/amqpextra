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
	d, _ := amqpextra.NewDialer(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		// process message
		msg.Ack(false)

		return nil
	})

	c, _ := d.Consumer(
		consumer.WithQueue("a_queue"),
		consumer.WithHandler(h),
	)

	// close consumer
	c.Close()

	// close dialer
	d.Close()

	// Output:
}

func ExampleNewConsumer() {
	// you can get connCh from dialer.ConnectionCh() method
	var connCh chan *amqpextra.Connection
	h := consumer.HandlerFunc(
		func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message
			msg.Ack(false)
			return nil
		})

	// create consumer
	c, err := amqpextra.NewConsumer(
		connCh,
		consumer.WithHandler(h),
		consumer.WithQueue("a_queue"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// close consumer
	c.Close()
	<-c.NotifyClosed()

	// Output:
}
