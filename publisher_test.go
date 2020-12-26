package amqpextra_test

import (
	"log"

	"context"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

func ExampleDialer_Publisher() {
	// open connection
	d, _ := amqpextra.NewDialer(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))

	// create publisher
	p, _ := d.Publisher()

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancelFunc()

	// publish a message
	p.Publish(publisher.Message{
		Key:     "test_queue",
		Context: ctx,
		Publishing: amqp.Publishing{
			Body: []byte(`{"foo": "fooVal"}`),
		},
	})

	// close publisher
	p.Close()

	// close connection
	d.Close()

	// Output:
}

func ExampleNewPublisher() {
	// you can get readyCh from dialer.ConnectionCh() method
	var connCh chan *amqpextra.Connection

	// create publisher
	p, err := amqpextra.NewPublisher(connCh)
	if err != nil {
		log.Fatal(err)
	}

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
