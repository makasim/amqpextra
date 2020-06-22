package consumer_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/amqpextra/test/e2e/helper/rabbitmq"

	"github.com/stretchr/testify/require"

	"github.com/makasim/amqpextra/test/e2e/helper/logger"
	"github.com/stretchr/testify/assert"

	"github.com/makasim/amqpextra"
	"github.com/streadway/amqp"
	"go.uber.org/goleak"
)

func TestPublishUnreadyNoResultChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	p.Publish(amqpextra.Publishing{
		WaitReady: false,
		Message:   amqp.Publishing{},
		ResultCh:  nil,
	})

	expected := `[ERROR] publisher not ready
`
	require.Equal(t, expected, l.Logs())
}

func TestPublishUnreadyWithResultChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	resultCh := make(chan error, 1)

	p.Publish(amqpextra.Publishing{
		WaitReady: false,
		Message:   amqp.Publishing{},
		ResultCh:  resultCh,
	})

	err := <-resultCh
	require.EqualError(t, err, "publisher not ready")

	expected := ``
	require.Equal(t, expected, l.Logs())
}

func TestPublishResultChannelUnbuffered(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	p.SetLogger(l)

	p.Close()

	assert.Panics(t, func() {
		p.Publish(amqpextra.Publishing{
			Context:  context.Background(),
			Message:  amqp.Publishing{},
			ResultCh: make(chan error),
		})
	})
}

func TestPublishToClosedPublisherNoResultChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	p.SetLogger(l)

	p.Close()

	p.Publish(amqpextra.Publishing{
		Context:  context.Background(),
		Message:  amqp.Publishing{},
		ResultCh: nil,
	})

	expected := `[ERROR] publisher stopped
`
	require.Equal(t, expected, l.Logs())
}

func TestPublishToClosedPublisherWithResultChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	p.SetLogger(l)
	p.Close()

	resultCh := make(chan error, 1)

	p.Publish(amqpextra.Publishing{
		Context:  context.Background(),
		Message:  amqp.Publishing{},
		ResultCh: resultCh,
	})

	err := <-resultCh
	require.EqualError(t, err, `publisher stopped`)

	expected := ``
	require.Equal(t, expected, l.Logs())
}

func TestPublishWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection)
	closeCh := make(chan *amqp.Error)

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer conn.Close()

	go func() {
		<-time.NewTimer(time.Second).C

		connCh <- conn
	}()

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	resultCh := make(chan error, 1)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancelFunc()

	before := time.Now().UnixNano()
	p.Publish(amqpextra.Publishing{
		Context:   ctx,
		WaitReady: true,
		Message:   amqp.Publishing{},
		ResultCh:  resultCh,
	})

	err = <-resultCh
	require.NoError(t, err)
	after := time.Now().UnixNano()

	expected := `[DEBUG] publisher started
`
	require.Equal(t, expected, l.Logs())

	require.GreaterOrEqual(t, after-before, int64(900000000))
	require.LessOrEqual(t, after-before, int64(1100000000))
}

func TestPublishConsumeWaitReady(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	q, err := ch.QueueDeclare("test-publish-with-wait-ready", true, false, false, false, amqp.Table{})
	require.NoError(t, err)

	connCh := make(chan *amqp.Connection, 1)

	go func() {
		<-time.NewTimer(100 * time.Millisecond).C

		connCh <- conn
	}()

	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	resultCh := make(chan error, 1)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFunc()

	p.Publish(amqpextra.Publishing{
		Key:       q.Name,
		Context:   ctx,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte(`testPayload`),
		},
		ResultCh: resultCh,
	})

	err = <-resultCh
	require.NoError(t, err)

	msgCh, err := ch.Consume(q.Name, "", true, false, false, false, amqp.Table{})
	require.NoError(t, err)

	msg, ok := <-msgCh
	require.True(t, ok)

	require.Equal(t, `testPayload`, string(msg.Body))
}

func TestPublishConsumeContextDeadline(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)

	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	resultCh := make(chan error, 1)

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()

	p.Publish(amqpextra.Publishing{
		Key:       "a_queue",
		Context:   ctx,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte(`testPayload`),
		},
		ResultCh: resultCh,
	})

	err := <-resultCh
	require.Equal(t, err, context.DeadlineExceeded)
}

func TestPublishConsumeContextCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)

	closeCh := make(chan *amqp.Error)

	p := amqpextra.NewPublisher(connCh, closeCh)
	defer p.Close()
	p.SetLogger(l)

	resultCh := make(chan error, 1)

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	p.Publish(amqpextra.Publishing{
		Key:       "a_queue",
		Context:   ctx,
		WaitReady: true,
		Message: amqp.Publishing{
			Body: []byte(`testPayload`),
		},
		ResultCh: resultCh,
	})

	err := <-resultCh
	require.Equal(t, err, context.Canceled)
}

func TestConcurrentlyPublishConsumeWhileConnectionLost(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.New()

	consumerConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	assert.NoError(t, err)
	defer consumerConn.Close()

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

	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())
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

			p := amqpextra.NewPublisher(connCh, closeCh)
			p.SetLogger(l)

			resultCh := make(chan error, 1)

			for {
				select {
				case <-ticker.C:
					p.Publish(amqpextra.Publishing{
						Key:       queue,
						WaitReady: true,
						ResultCh:  resultCh,
					})

					if err := <-resultCh; err == nil {
						atomic.AddUint32(&countPublished, 1)
					} else {
						t.Errorf("publish errored: %s", err)
					}
				case <-timer.C:
					p.Close()

					return
				}
			}
		}(conn, queue, &wg)
	}

	var countConsumed uint32
	for i := 0; i < 5; i++ {
		wg.Add(1)

		timer := time.NewTimer(time.Second * 11)

		go rabbitmq.ConsumeReconnect(consumerConn, timer, queue, &countConsumed, &wg)
	}

	wg.Wait()

	expected := `[DEBUG] connection established
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] connection established
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher started
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
[DEBUG] publisher stopped
`
	require.Equal(t, expected, l.Logs())

	require.GreaterOrEqual(t, countPublished, uint32(200))
	require.LessOrEqual(t, countPublished, uint32(520))

	require.GreaterOrEqual(t, countConsumed, uint32(200))
	require.LessOrEqual(t, countConsumed, uint32(520))
}
