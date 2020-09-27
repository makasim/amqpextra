package amqpextra_test

import (
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleConnector_Publisher() {
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
	// you can get estCh (established connections channel) conn.Ready() method
	var estCh chan amqpextra.Established

	// create publisher
	p := amqpextra.NewPublisher(estCh)

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
