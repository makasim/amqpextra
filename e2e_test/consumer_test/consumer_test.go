package consumer_test_test

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"go.uber.org/goleak"

	"time"

	"context"

	"math/rand"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumeWhileConnectionClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	amqpConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer amqpConn.Close()

	q := rabbitmq.Queue(amqpConn)
	for i := 0; i < 999; i++ {
		rabbitmq.Publish(amqpConn, `Hello!`, q)
	}
	rabbitmq.Publish(amqpConn, `Last!`, q)
	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rand.Int63n(10000000))
	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
		Properties: amqp.Table{
			"connection_name": connName,
		},
	})
	defer conn.Close()
	conn.Start()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	timer := time.NewTicker(time.Second * 5)
	defer timer.Stop()
waitOpened:
	for {
		select {
		case <-ticker.C:
			if rabbitmq.IsOpened(connName) {
				break waitOpened
			}
		case <-timer.C:
			t.Fatalf("connection %s is not opened", connName)
		}
	}

	resultCh := make(chan error, 1)
	c := conn.Consumer(q, consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		resultCh <- msg.Ack(false)

		if string(msg.Body) == "Last!" {
			close(resultCh)
		}

		return nil
	}))
	go c.Run()

	assertReady(t, c)

	count := 0
	errorCount := 0
	for res := range resultCh {
		if res == nil {
			count++
		} else {
			errorCount++
		}

		if (count + errorCount) == 300 {
			time.Sleep(time.Millisecond * 100)
			require.True(t, rabbitmq.CloseConn(connName))
		}
	}

	assert.GreaterOrEqual(t, count, 995)

	conn.Close()
	<-c.Closed()
	//
	time.Sleep(time.Millisecond * 100)
}

func assertReady(t *testing.T, p *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-p.Ready():
	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}
