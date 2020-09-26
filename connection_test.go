package amqpextra_test

import (
	"log"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func ExampleConnection_ConnCh() {
	conn := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})

	connCh, closeCh := conn.ConnCh()

	go func() {
	L1:

		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					// connection permanently closed
					return
				}

				ch, err := conn.Channel()
				if err != nil {
					return
				}

				ticker := time.NewTicker(time.Second * 5)
				for {
					select {
					case <-ticker.C:
						// do some stuff
						err := ch.Publish("", "a_queue", false, false, amqp.Publishing{
							Body: []byte("I've got some news!"),
						})
						if err != nil {
							log.Print(err)
						}
					case <-closeCh:
						// connection is lost. let`s get new one
						continue L1
					}
				}
			}

		}
	}()

	time.Sleep(time.Second)
	conn.Close()

	// Output:
}
