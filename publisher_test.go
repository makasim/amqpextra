package amqpextra_test

import (
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleConnection_Publisher() {
	// open connection
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Discard)

	// create publisher
	p := conn.Publisher()

	// publish a message
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	// close publisher
	p.Close()
	<-p.Closed()

	// close connection
	conn.Close()

	// Output:
}

func ExampleNewPublisher() {
	// you can get connCh and closeCh from conn.ConnCh() method
	var connCh chan *amqp.Connection
	var closeCh chan *amqp.Error

	// create publisher
	p := amqpextra.NewPublisher(connCh, closeCh)

	// publish a message
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	// close publisher
	p.Close()
	<-p.Closed()

	// Output:
}
