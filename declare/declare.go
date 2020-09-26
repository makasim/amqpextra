package declare

import (
	"context"

	"log"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
)

func TempQueue(
	ctx context.Context,
	c *amqpextra.Connector,
) (amqp.Queue, error) {
	return Queue(
		ctx,
		c,
		"",
		false,
		true,
		true,
		false,
		amqp.Table{},
	)
}

func Queue(
	ctx context.Context,
	c *amqpextra.Connector,
	name string,
	durable,
	autDelete,
	exclusive,
	noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	select {
	case <-ctx.Done():
		return amqp.Queue{}, ctx.Err()
	case <-c.Ready():
		conn, err := c.Conn()
		if err != nil {
			return amqp.Queue{}, err
		}

		ch, err := conn.Channel()
		if err != nil {
			return amqp.Queue{}, err
		}
		defer func() {
			if closeErr := ch.Close(); closeErr != nil {
				log.Print("amqpextra: declare queue: ch close: %w", closeErr)
			}
		}()

		q, err := ch.QueueDeclare(name, durable, autDelete, exclusive, noWait, args)
		if err != nil {
			return amqp.Queue{}, err
		}

		return q, nil
	}
}
