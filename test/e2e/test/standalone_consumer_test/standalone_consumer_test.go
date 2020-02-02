package standalone_consumer_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/amqpextra/test/e2e/helper/rabbitmq"
	"github.com/stretchr/testify/require"

	"github.com/makasim/amqpextra"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra/test/e2e/helper/logger"
)

func TestCloseConsumerWhenConnChannelClosed(t *testing.T) {
	l := logger.New()

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	connCh := make(chan *amqp.Connection, 1)
	connCh <- conn

	closeCh := make(chan *amqp.Error)

	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return nil
	})

	go func() {
		<-time.NewTimer(time.Second).C

		close(closeCh)
		close(connCh)
	}()

	c := amqpextra.NewConsumer(rabbitmq.Queue(conn), worker, connCh, closeCh)
	c.SetLogger(l)

	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())
}

func TestGetNewConsumerOnErrorInCloseCh(t *testing.T) {
	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)
	closeCh := make(chan *amqp.Error)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	queue := rabbitmq.Queue(conn)

	go func() {
		l.Printf("[DEBUG] send fresh connection")
		connCh <- conn

		<-time.NewTimer(time.Millisecond * 100).C

		l.Printf("[DEBUG] send connection is closed")
		closeCh <- amqp.ErrClosed

		<-time.NewTimer(time.Millisecond * 100).C
		l.Printf("[DEBUG] trying reconnect")

		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
		require.NoError(t, err)

		l.Printf("[DEBUG] reconnected")
		connCh <- conn

		<-time.NewTimer(time.Millisecond * 100).C

		l.Printf("[DEBUG] close connection permanently")

		close(connCh)
		close(closeCh)
	}()

	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return nil
	})

	c := amqpextra.NewConsumer(queue, worker, connCh, closeCh)
	c.SetLogger(l)
	defer c.Close()

	c.Run()

	expected := `[DEBUG] send fresh connection
[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] send connection is closed
[DEBUG] workers stopped
[DEBUG] trying reconnect
[DEBUG] reconnected
[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] close connection permanently
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())
}

func TestCloseConsumerByContext(t *testing.T) {
	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)
	closeCh := make(chan *amqp.Error)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	queue := rabbitmq.Queue(conn)

	ctx, cancelFunc := context.WithCancel(context.Background())

	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return nil
	})

	go func() {
		connCh <- conn

		<-time.NewTimer(time.Second).C

		l.Printf("[DEBUG] close context")
		cancelFunc()
	}()

	c := amqpextra.NewConsumer(queue, worker, connCh, closeCh)
	c.SetContext(ctx)
	c.SetLogger(l)

	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] close context
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())
}

func TestCloseChannelOnAlreadyClosedConnection(t *testing.T) {
	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)
	closeCh := make(chan *amqp.Error)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	queue := rabbitmq.Queue(conn)

	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		return nil
	})

	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		connCh <- conn

		<-time.NewTimer(time.Second).C

		cancelFunc()
		require.NoError(t, conn.Close())
	}()

	c := amqpextra.NewConsumer(queue, worker, connCh, closeCh)
	c.SetLogger(l)
	c.SetContext(ctx)

	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())

	require.NotContains(t, l.Logs(), "Exception (504) Reason: \"channel/connection is not open\"\n[DEBUG] consumer stopped\n")
}

func TestConsumeOneAndCloseConsumer(t *testing.T) {
	l := logger.New()

	connCh := make(chan *amqp.Connection, 1)
	closeCh := make(chan *amqp.Error)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)

	queue := rabbitmq.Queue(conn)
	rabbitmq.Publish(conn, "testbdy", queue)

	connCh <- conn

	var c *amqpextra.Consumer
	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		l.Printf("[DEBUG] got message %s", msg.Body)

		msg.Ack(false)

		c.Close()

		return nil
	})

	c = amqpextra.NewConsumer(queue, worker, connCh, closeCh)
	c.SetLogger(l)
	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] got message testbdy
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())
}

func TestConcurrentlyPublishConsumeWhileConnectionLost(t *testing.T) {
	l := logger.New()

	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())

	connCh := make(chan *amqp.Connection, 1)
	closeCh := make(chan *amqp.Error)
	conn, err := amqp.DialConfig("amqp://guest:guest@rabbitmq:5672/amqpextra", amqp.Config{
		Properties: amqp.Table{
			"connection_name": connName,
		},
	})
	require.NoError(t, err)
	defer conn.Close()

	publishConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer publishConn.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func(_ string, wg *sync.WaitGroup) {
		defer wg.Done()

		connCh <- conn

		<-time.NewTimer(time.Second * 2).C
		l.Printf("[DEBUG] simulate lost connection")
		closeCh <- amqp.ErrClosed

		<-time.NewTimer(time.Millisecond * 100).C
		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
		require.NoError(t, err)

		l.Printf("[DEBUG] get new connection")

		connCh <- conn
	}(connName, &wg)

	queue := rabbitmq.Queue(conn)

	var countPublished uint32
	for i := 0; i < 5; i++ {
		wg.Add(1)

		ticker := time.NewTicker(time.Millisecond * 100)
		timer := time.NewTimer(time.Second * 4)

		go rabbitmq.PublishTimer(publishConn, timer, ticker, queue, &countPublished, &wg)
	}

	var countConsumed uint32

	worker := amqpextra.WorkerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		atomic.AddUint32(&countConsumed, 1)

		msg.Ack(false)

		return nil
	})

	c := amqpextra.NewConsumer(queue, worker, connCh, closeCh)
	c.SetWorkerNum(5)
	c.SetLogger(l)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-time.NewTimer(time.Second * 5).C

		c.Close()
	}()

	c.Run()
	wg.Wait()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] simulate lost connection
[DEBUG] workers stopped
[DEBUG] get new connection
[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	require.Equal(t, expected, l.Logs())

	require.GreaterOrEqual(t, countPublished, uint32(100))
	require.LessOrEqual(t, countPublished, uint32(220))

	require.GreaterOrEqual(t, countConsumed, uint32(100))
	require.LessOrEqual(t, countConsumed, uint32(220))
}
