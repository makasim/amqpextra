package amqpextra_test

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleDialer_Publisher() {
	// open connection
	dialer, err := amqpextra.New(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))
	if err != nil {
		log.Fatal(err)
	}

	// create publisher
	p := dialer.Publisher()

	// publish a message
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	// close publisher
	p.Close()
	<-p.NotifyClosed()

	// close connection
	dialer.Close()

	// Output:
}

func ExampleNewPublisher() {
	// you can get readyCh from dialer.NotifyReady() method
	var readyCh chan *amqpextra.Connection

	// create publisher
	p := amqpextra.NewPublisher(readyCh)

	// publish a message
	go p.Publish(publisher.Message{
		Key: "test_queue",
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	// close publisher
	p.Close()
	<-p.NotifyClosed()

	// Output:
}
