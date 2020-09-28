package amqpextra_test

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleDialer_Publisher() {
	// open connection
	conn, err := amqpextra.Dial(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))
	if err != nil {
		log.Fatal(err)
	}

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
	var estCh chan amqpextra.Ready

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
