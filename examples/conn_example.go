package examples

import (
	"log"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
)

func ConnExample() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	conn.SetLogger(logger.Std)

	connCh, closeCh := conn.ConnCh()

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
			case msg, ok := <-msgCh:
				if !ok {
					log.Printf("amqp: consumption stopped")

					continue L1
				}
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
