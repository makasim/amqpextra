package e2e_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/goleak"

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
	stateCh := make(chan amqpextra.State, 1)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithNotify(stateCh),
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

	consumerStateCh := make(chan consumer.State, 1)
	c, err := dialer.Consumer(
		consumer.WithQueue(q),
		consumer.WithHandler(h),
		consumer.WithNotify(consumerStateCh))
	require.NoError(t, err)

	assertConsumerReady(t, consumerStateCh)

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

	dialerStateCh := make(chan amqpextra.State, 1)

	dialer, err := amqpextra.NewDialer(
		amqpextra.WithNotify(dialerStateCh),
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
	)
	require.NoError(t, err)
	defer dialer.Close()

	assertDialerReady(t, dialerStateCh)

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

	stateCh := make(chan consumer.State, 1)
	c, err := dialer.Consumer(
		consumer.WithNotify(stateCh),
		consumer.WithExchange(exchangeName, ""),
		consumer.WithHandler(h),
	)
	require.NoError(t, err)
	defer c.Close()

	assertConsumerReady(t, stateCh)

	err = ch.PublishWithContext(context.Background(), exchangeName,
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

func assertConsumerReady(t *testing.T, stateCh <-chan consumer.State) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state, ok := <-stateCh:
		if !ok {
			require.Equal(t, "permanently closed", state.Ready.Queue)
			return
		}

		require.Nil(t, state.Unready)

		require.NotNil(t, state.Ready)

	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}
