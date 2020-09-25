package amqpextra_test

import (
	"fmt"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleConnection_Publisher() {
	fmt.Println("open connection")
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Discard)

	fmt.Println("create publisher")
	p := conn.Publisher()

	fmt.Println("publish a message")
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	fmt.Println("close publisher")
	p.Close()
	<-p.Closed()

	fmt.Println("close connection")
	conn.Close()

	// Output:
	// open connection
	// create publisher
	// publish a message
	// close publisher
	// close connection
}

func ExampleNewPublisher() {
	fmt.Println("you can get connCh and closeCh from conn.ConnCh() method")
	var connCh chan *amqp.Connection
	var closeCh chan *amqp.Error

	fmt.Println("create publisher")
	p := amqpextra.NewPublisher(connCh, closeCh)

	fmt.Println("publish a message")
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	fmt.Println("close publisher")
	p.Close()
	<-p.Closed()

	// Output:
	// you can get connCh and closeCh from conn.ConnCh() method
	// create publisher
	// publish a message
	// close publisher
}
