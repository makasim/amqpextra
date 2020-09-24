package consumer_test_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"

	"github.com/makasim/amqpextra"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra/consumer"
	logger2 "github.com/makasim/amqpextra/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestConcurrentlyPublishConsumeWhileConnectionLost(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger2.NewTest()

	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())

	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
		Properties: amqp.Table{
			"connection_name": connName,
		},
	})
	defer conn.Close()

	conn.SetLogger(l)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(connName string, wg *sync.WaitGroup) {
		defer wg.Done()

		<-time.NewTimer(time.Second * 5).C

		if !assert.True(t, rabbitmq.CloseConn(connName)) {
			return
		}
	}(connName, &wg)

	queue := rabbitmq.Queue2(conn)

	var countPublished uint32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(extraconn *amqpextra.Connection, queue string, wg *sync.WaitGroup) {
			defer wg.Done()

			connCh, closeCh := extraconn.ConnCh()

			ticker := time.NewTicker(time.Millisecond * 100)
			defer ticker.Stop()
			timer := time.NewTimer(time.Second * 10)
			defer timer.Stop()

		L1:
			for conn := range connCh {
				ch, err := conn.Channel()
				require.NoError(t, err)

				_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
				require.NoError(t, err)

				for {
					select {
					case <-ticker.C:
						err := ch.Publish("", queue, false, false, amqp.Publishing{})

						if err == nil {
							atomic.AddUint32(&countPublished, 1)
						}
					case <-closeCh:
						break
					case <-timer.C:
						break L1
					}
				}

			}

		}(conn, queue, &wg)
	}

	var countConsumed uint32

	c := conn.Consumer(queue, consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		atomic.AddUint32(&countConsumed, 1)

		msg.Ack(false)

		return nil
	}))

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-time.NewTimer(time.Second * 11).C

		c.Close()
	}()

	c.Run()
	wg.Wait()

	expected := `[DEBUG] connection established
[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] connection established
[DEBUG] consumer starting
[DEBUG] workers started
`
	require.Contains(t, l.Logs(), expected)

	require.GreaterOrEqual(t, countPublished, uint32(200))
	require.LessOrEqual(t, countPublished, uint32(520))

	require.GreaterOrEqual(t, countConsumed, uint32(200))
	require.LessOrEqual(t, countConsumed, uint32(520))
}
