package declare_test

import (
	"fmt"
	"testing"
	"time"

	"context"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDeclareQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	defer conn.Close()

	expectedQueue := fmt.Sprintf("declare-queue-%d", time.Now().UnixNano())

	q, err := amqpextra.DeclareQueue(
		context.Background(),
		conn,
		expectedQueue,
		false,
		false,
		false,
		false,
		amqp.Table{},
	)

	require.NoError(t, err)
	require.Equal(t, expectedQueue, q.Name)
}

func TestDeclareTempQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	defer conn.Close()

	q, err := amqpextra.DeclareTempQueue(context.Background(), conn)

	require.NoError(t, err)
	require.NotEmpty(t, q.Name)
}
