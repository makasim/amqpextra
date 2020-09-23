package publisher_test

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	"github.com/makasim/amqpextra/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPublishWhileConnectionClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())
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
	p := conn.Publisher()
	assertReady(t, p)

	count := 0
	errorCount := 0
	resultCh := make(chan error, 1)
	for i := 0; i < 1000; i++ {
		if i == 300 {
			time.Sleep(time.Millisecond * 100)
			require.True(t, rabbitmq.CloseConn(connName))
		}

		p.Publish(publisher.Message{
			ResultCh:  resultCh,
			WaitReady: true,
		})

		res := <-resultCh
		if res == nil {
			count++
		} else {
			errorCount++
		}
	}

	assert.GreaterOrEqual(t, count, 995)

	conn.Close()
	<-p.Closed()
}

func assertReady(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-p.Ready():
	case <-timer.C:
		t.Fatal("publisher must be ready")
	}
}
