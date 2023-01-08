package e2e_test

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/amqpextra/consumerpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/goleak"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	"github.com/stretchr/testify/require"
)

func TestConsumePool(t *testing.T) {
	defer goleak.VerifyNone(t)

	amqpConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer amqpConn.Close()

	var consumedCounter int64

	q := rabbitmq.Queue(amqpConn)
	for i := 0; i < 2000; i++ {
		rabbitmq.Publish(amqpConn, `Hello!`, q)
	}

	cp, err := consumerpool.New(
		[]amqpextra.Option{
			amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		},
		[]consumer.Option{
			consumer.WithQos(10, false),
			consumer.WithWorker(consumer.NewParallelWorker(10)),
			consumer.WithDeclareQueue(q, true, false, false, false, nil),
			consumer.WithHandler(consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
				atomic.AddInt64(&consumedCounter, 1)
				time.Sleep(time.Millisecond * 100)
				if err := msg.Ack(false); err != nil {
					log.Fatal(err)
				}

				return nil
			})),
		},
		[]consumerpool.Option{
			consumerpool.WithMinSize(1),
			consumerpool.WithMaxSize(10),
		},
	)
	require.NoError(t, err)
	defer cp.Close()

	time.Sleep(time.Second * 6)
	cp.Close()
	<-cp.NotifyClosed()

	require.Equal(t, int64(2000), atomic.LoadInt64(&consumedCounter))
}
