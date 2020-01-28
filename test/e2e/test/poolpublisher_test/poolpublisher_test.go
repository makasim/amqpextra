package consumer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/test/e2e/helper/logger"
	"github.com/streadway/amqp"
)

func TestPublishToFirstConnection(t *testing.T) {
	l := logger.New()

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	connCh1 := make(chan *amqp.Connection, 1)
	closeCh1 := make(chan *amqp.Error)
	p1 := amqpextra.NewPublisher(connCh1, closeCh1)

	connCh2 := make(chan *amqp.Connection, 1)
	closeCh2 := make(chan *amqp.Error)
	p2 := amqpextra.NewPublisher(connCh2, closeCh2)

	connCh1 <- conn

	p := amqpextra.NewPoolPublisher([]*amqpextra.Publisher{p1, p2})

	resultCh := make(chan error)

	p.Publish(amqpextra.Publishing{
		WaitReady: false,
		Message:   amqp.Publishing{},
		ResultCh:  nil,
	})

	err = <-resultCh
	require.EqualError(t, err, "foo")

	expected := `[ERROR] publish: Exception (504) Reason: "channel/connection is not open"
`
	require.Equal(t, expected, l.Logs())
}

func TestPublishToSecondConnection(t *testing.T) {
	l := logger.New()

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	connCh1 := make(chan *amqp.Connection, 1)
	closeCh1 := make(chan *amqp.Error)
	p1 := amqpextra.NewPublisher(connCh1, closeCh1)

	connCh2 := make(chan *amqp.Connection, 1)
	closeCh2 := make(chan *amqp.Error)
	p2 := amqpextra.NewPublisher(connCh2, closeCh2)

	connCh2 <- conn

	p := amqpextra.NewPoolPublisher([]*amqpextra.Publisher{p1, p2})

	resultCh := make(chan error)

	p.Publish(amqpextra.Publishing{
		WaitReady: false,
		Message:   amqp.Publishing{},
		ResultCh:  nil,
	})

	err = <-resultCh
	require.EqualError(t, err, "foo")

	expected := `[ERROR] publish: Exception (504) Reason: "channel/connection is not open"
`
	require.Equal(t, expected, l.Logs())
}
