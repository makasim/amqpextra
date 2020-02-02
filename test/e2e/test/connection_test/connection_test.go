package connection_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/makasim/amqpextra/test/e2e/helper/rabbitmq"

	"context"

	"github.com/makasim/amqpextra/test/e2e/helper/assertlog"
	"github.com/makasim/amqpextra/test/e2e/helper/logger"
	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCouldNotConnect(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@127.0.0.1:5672/amqpextra"})
	defer conn.Close()

	conn.SetReconnectSleep(time.Millisecond * 750)
	conn.SetLogger(l)

	connCh, closeCh := conn.Get()
	select {
	case <-connCh:
		t.Fatalf("it should not happen")
	case <-closeCh:
		t.Fatalf("it should not happen")
	case <-time.NewTimer(time.Second * 2).C:
		expected := `[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
`
		require.Equal(t, expected, l.Logs())
	}
}

func TestConnectRoundRobinServers(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{
		"amqp://guest:guest@127.0.0.1:5672/amqpextra",
		"amqp://another:another@127.0.0.1:5677/another",
	})
	defer conn.Close()

	conn.SetReconnectSleep(time.Millisecond * 750)
	conn.SetLogger(l)

	connCh, closeCh := conn.Get()
	select {
	case <-connCh:
		t.Fatalf("it should not happen")
	case <-closeCh:
		t.Fatalf("it should not happen")
	case <-time.NewTimer(time.Second * 3).C:
		expected := `[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[ERROR] dial tcp 127.0.0.1:5677: connect: connection refused
[DEBUG] try reconnect
[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[ERROR] dial tcp 127.0.0.1:5677: connect: connection refused
`
		require.Equal(t, expected, l.Logs())
	}
}

func TestConnectToSecondServer(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{
		"amqp://guest:guest@127.0.0.1:5672/amqpextra",
		"amqp://guest:guest@rabbitmq:5672/amqpextra",
	})
	defer conn.Close()

	conn.SetReconnectSleep(time.Millisecond * 750)
	conn.SetLogger(l)

	connCh, closeCh := conn.Get()
	select {
	case _, ok := <-connCh:
		require.True(t, ok)

		expected := `[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[DEBUG] connection established
`
		require.Equal(t, expected, l.Logs())
	case <-closeCh:
		t.Fatalf("it should not happen")
	}
}

func TestCloseConnExplicitly(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})

	go func() {
		<-time.NewTimer(time.Second).C

		conn.Close()
	}()

	conn.SetLogger(l)

	connCh, closeCh := conn.Get()

	_, ok := <-connCh
	require.True(t, ok)

	_, ok = <-closeCh
	require.False(t, ok)

	_, ok = <-connCh
	require.False(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection is closed
`
	require.Equal(t, expected, l.Logs())
}

func TestCloseConnByContext(t *testing.T) {
	l := logger.New()

	ctx, cancel := context.WithCancel(context.Background())

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	conn.SetContext(ctx)

	go func() {
		<-time.NewTimer(time.Second).C

		cancel()
	}()

	conn.SetLogger(l)

	connCh, closeCh := conn.Get()

	_, ok := <-connCh
	require.True(t, ok)

	_, ok = <-closeCh
	require.False(t, ok)

	_, ok = <-connCh
	require.False(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection is closed
`
	require.Equal(t, expected, l.Logs())
}

func TestReconnectIfClosedByUser(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	conn.SetLogger(l)

	connCh, closeCh := conn.Get()

	realconn, ok := <-connCh
	require.True(t, ok)

	require.NoError(t, realconn.Close())

	err, ok := <-closeCh
	require.True(t, ok)
	require.EqualError(t, err, "Exception (504) Reason: \"channel/connection is not open\"")

	_, ok = <-connCh
	require.True(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection established
`
	require.Equal(t, expected, l.Logs())
}

func TestReconnectIfClosedByServer(t *testing.T) {
	l := logger.New()

	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())

	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
		Properties: amqp.Table{
			"connection_name": connName,
		},
	})
	defer conn.Close()

	conn.SetLogger(l)

	connCh, closeCh := conn.Get()

	_, ok := <-connCh
	require.True(t, ok)

	assertlog.WaitContainsOrFatal(t, rabbitmq.OpenedConns, connName, time.Second*5)

	if !assert.True(t, rabbitmq.CloseConn(connName)) {
		return
	}

	err, ok := <-closeCh
	require.True(t, ok)
	require.EqualError(t, err, "Exception (320) Reason: \"CONNECTION_FORCED - Closed via management plugin\"")

	_, ok = <-connCh
	require.True(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection established
`
	require.Equal(t, expected, l.Logs())
}

func TestNotReadingFromCloseCh(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	conn.SetLogger(l)

	connCh, _ := conn.Get()

	realconn, ok := <-connCh
	require.True(t, ok)

	require.NoError(t, realconn.Close())

	time.Sleep(time.Millisecond * 100)
	// <-closeCh

	realconn, ok = <-connCh
	require.True(t, ok)

	require.NoError(t, realconn.Close())

	// <-closeCh

	expected := `[DEBUG] connection established
[DEBUG] connection established
`
	require.Equal(t, expected, l.Logs())
}

func TestConnPublishConsume(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	defer conn.Close()

	conn.SetLogger(l)

	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())

	connCh, closeCh := conn.Get()

	select {
	case conn, ok := <-connCh:
		require.True(t, ok)

		ch, err := conn.Channel()
		require.NoError(t, err)

		q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
		require.NoError(t, err)

		err = ch.Publish("", queue, false, false, amqp.Publishing{
			Body: []byte("testbdy"),
		})
		require.NoError(t, err)

		msgCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
		require.NoError(t, err)

		msg, ok := <-msgCh
		require.True(t, ok)

		require.NoError(t, msg.Ack(false))
		require.Equal(t, "testbdy", string(msg.Body))

		expected := `[DEBUG] connection established
`
		require.Equal(t, expected, l.Logs())
	case <-closeCh:
		t.Fatalf("connection is closed")
	}
}

func TestConcurrentlyPublishConsumeWhileConnectionLost(t *testing.T) {
	l := logger.New()

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

		ticker := time.NewTicker(time.Millisecond * 100)
		timer := time.NewTimer(time.Second * 10)

		go rabbitmq.PublishTimerReconnect(conn, timer, ticker, queue, &countPublished, &wg)
	}

	var countConsumed uint32
	for i := 0; i < 5; i++ {
		wg.Add(1)

		timer := time.NewTimer(time.Second * 11)

		go rabbitmq.ConsumeTimerReconnect(conn, timer, queue, &countConsumed, &wg)
	}

	wg.Wait()

	expected := `[DEBUG] connection established
[DEBUG] connection established
`
	require.Equal(t, expected, l.Logs())

	assert.GreaterOrEqual(t, countPublished, uint32(200))
	assert.LessOrEqual(t, countPublished, uint32(520))

	assert.GreaterOrEqual(t, countConsumed, uint32(200))
	assert.LessOrEqual(t, countConsumed, uint32(520))
}
