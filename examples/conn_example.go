package main

import (
	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	connextra := amqpextra.New(
		func() (*amqp.Connection, error) {
			return amqp.Dial("amqp://guest:guest@localhost:5672/gitcall")
		},
		log.Printf,
		log.Printf,
	)
	defer connextra.Close()

	connCh, closeCh := connextra.Get()

	L1:
	for conn := range connCh {
		ch, err := conn.Channel()
		if err != nil {
			log.Printf("amqp: create channel: %s", err)

			return
		}

		q, err := ch.QueueDeclare("test-queue", true, false, false, false, nil)
		if err != nil {
			log.Printf("amqp: declare queue: %s", err)

			return
		}

		msgCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("amqp: consume: %s", err)

			return
		}

		log.Printf("amqp: consumption started")
		for {
			select {
			case msg := <-msgCh:
				// process message here
				log.Printf(string(msg.Body))
				msg.Ack(false)
			case err, ok := <-closeCh:
				if !ok {
					log.Printf("amqp: consumption stopped")

					continue L1
				}

				log.Printf("amqp: consumption stopped: %v", err)

				continue L1
			}
		}
	}

	log.Printf("connection permanently closed")
}
