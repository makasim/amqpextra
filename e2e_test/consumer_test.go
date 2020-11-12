package e2e_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/streadway/amqp"
	"go.uber.org/goleak"

	"time"

	"context"

	"crypto/rand"

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
	rnum, err := rand.Int(rand.Reader, big.NewInt(10000000))
	require.NoError(t, err)
	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rnum)
	readyCh := make(chan struct{}, 1)
	unreadyCh := make(chan error, 1)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithNotify(readyCh, unreadyCh),
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": connName,
		}),
	)
	require.NoError(t, err)
	defer dialer.Close()

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
	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		resultCh <- msg.Ack(false)

		if string(msg.Body) == "Last!" {
			close(resultCh)
		}

		return nil
	})

	c, err := dialer.Consumer(consumer.WithQueue(q), consumer.WithHandler(h))
	require.NoError(t, err)

	assertConsumerReady(t, readyCh)

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

	dialer.Close()
	<-c.NotifyClosed()
	//
	time.Sleep(time.Millisecond * 100)
}

func TestConsumerWithExchange(t *testing.T) {
	amqpConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	require.NoError(t, err)
	defer amqpConn.Close()

	ch, err := amqpConn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	rnum, err := rand.Int(rand.Reader, big.NewInt(10000000))
	require.NoError(t, err)
	exchangeName := fmt.Sprintf("exchange%d%d", time.Now().UnixNano(), rnum)

	dialerReadyCh := make(chan struct{}, 1)
	dialerUnreadyCh := make(chan error, 1)

	dialer, err := amqpextra.NewDialer(
		amqpextra.WithNotify(dialerReadyCh, dialerUnreadyCh),
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
	)
	require.NoError(t, err)

	assertConsumerReady(t, dialerReadyCh)

	gotMsg := make(chan struct{})
	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		close(gotMsg)
		return nil
	})

	err = ch.ExchangeDeclare(
		exchangeName,
		amqp.ExchangeFanout,
		false,
		true,
		false,
		false,
		nil,
	)
	require.NoError(t, err)
	readyCh := make(chan consumer.Ready, 1)
	unreadyCh := make(chan error, 1)
	c, err := dialer.Consumer(
		consumer.WithNotify(readyCh, unreadyCh),
		consumer.WithExchange(exchangeName, ""),
		consumer.WithHandler(h),
	)
	require.NoError(t, err)
	defer c.Close()

	assertConsumerReadyQueue(t, readyCh)

	err = ch.Publish(exchangeName,
		"",
		false,
		false,
		amqp.Publishing{Body: []byte("aMessage")},
	)
	require.NoError(t, err)
	timer := time.NewTimer(time.Second)

	
	select {
	case <-timer.C:
		t.Fatal("message must be received")
	case <-gotMsg:     
        }
	

	c.Close()
	<-c.NotifyClosed()
	dialer.Close()
}

func assertConsumerReadyQueue(t *testing.T, readyCh chan consumer.Ready) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-readyCh:
	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}

func assertConsumerReady(t *testing.T, readyCh chan struct{}) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-readyCh:
	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}
