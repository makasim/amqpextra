package test

import (
	"fmt"
	"testing"
	"time"

	"context"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra"
	"github.com/stretchr/testify/assert"
)

func TestCouldNotConnect(t *testing.T) {
	l := NewLogger()

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
		assert.Equal(t, expected, l.Logs())
	}
}

func TestConnectToSecondServer(t *testing.T) {
	l := NewLogger()

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
		assert.True(t, ok)

		expected := `[ERROR] dial tcp 127.0.0.1:5672: connect: connection refused
[DEBUG] try reconnect
[DEBUG] connection established
`
		assert.Equal(t, expected, l.Logs())
	case <-closeCh:
		t.Fatalf("it should not happen")
	}
}

func TestCloseConnExplicitly(t *testing.T) {
	l := NewLogger()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})

	go func() {
		<-time.NewTimer(time.Second).C

		conn.Close()
	}()

	conn.SetLogger(l)

	connCh, closeCh := conn.Get()

	_, ok := <-connCh
	assert.True(t, ok)

	<-closeCh

	_, ok = <-connCh
	assert.False(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection is closed
`
	assert.Equal(t, expected, l.Logs())
}

func TestCloseConnByContext(t *testing.T) {
	l := NewLogger()

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
	assert.True(t, ok)

	<-closeCh

	_, ok = <-connCh
	assert.False(t, ok)

	expected := `[DEBUG] connection established
[DEBUG] connection is closed
`
	assert.Equal(t, expected, l.Logs())
}

func TestConnPublishConsume(t *testing.T) {
	l := NewLogger()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	defer conn.Close()

	conn.SetLogger(l)

	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())

	connCh, closeCh := conn.Get()

	select {
	case conn, ok := <-connCh:
		ch, err := conn.Channel()
		assert.NoError(t, err)

		q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
		assert.NoError(t, err)

		err = ch.Publish("", queue, false, false, amqp.Publishing{
			Body: []byte("testbdy"),
		})
		assert.NoError(t, err)

		msgCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
		assert.NoError(t, err)

		msg, ok := <-msgCh
		assert.True(t, ok)

		assert.NoError(t, msg.Ack(false))
		assert.Equal(t, "testbdy", string(msg.Body))

		expected := `[DEBUG] connection established
`
		assert.Equal(t, expected, l.Logs())
	case <-closeCh:
		t.Fatalf("connection is closed")
	}
}
