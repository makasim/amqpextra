package amqpextra_test

import (
	"log"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

// nolint:gosimple // the purpose of select case is to stress the connCh close case.
func ExampleConnector_Ready() {
	conn, err := amqpextra.Dial([]string{"amqp://guest:guest@localhost:5672/%2f"})
	if err != nil {
		log.Fatal(err)
	}

	estCh := conn.Ready()
	go func() {
	L1:

		for {
			select {
			case est, ok := <-estCh:
				if !ok {
					// connection permanently closed
					return
				}

				ch, err := est.Conn().Channel()
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
					case <-est.NotifyClose():
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
