package amqpextra_test

import (
	"context"

	"fmt"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

func ExampleConnection_Consumer() {
	fmt.Println("open connection")
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Discard)

	fmt.Println("create consumer")
	c := conn.Consumer(
		"some_queue",
		consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
			// process message

			msg.Ack(false)

			return nil
		}),
	)
	fmt.Println("run consumer")
	go c.Run()

	fmt.Println("close consumer")
	c.Close()
	<-c.Closed()

	fmt.Println("close connection")
	conn.Close()

	// Output:
	// open connection
	// create consumer
	// run consumer
	// close consumer
	// close connection
}

func ExampleNewConsumer() {
	fmt.Println("you can get connCh and closeCh from conn.ConnCh() method")
	var connCh chan *amqp.Connection
	var closeCh chan *amqp.Error

	fmt.Println("create consumer")
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
	fmt.Println("run consumer")
	go c.Run()

	fmt.Println("close consumer")
	c.Close()
	<-c.Closed()

	// Output:
	// you can get connCh and closeCh from conn.ConnCh() method
	// create consumer
	// run consumer
	// close consumer
}
