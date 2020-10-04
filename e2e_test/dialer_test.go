package e2e_test

import (
	"fmt"
	"math/rand"
	"testing"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDialerReconnectWhenClosedByClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
	)
	require.NoError(t, err)
	defer dialer.Close()

	assertConnectionReady(t, dialer)
	conn := <-dialer.ConnectionCh()

	go func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, conn.AMQPConnection().Close())
	}()
	assertConnectionUnready(t, dialer)

	assertConnectionReady(t, dialer)
}

func TestDialerReconnectWhenClosedByServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rand.Int63n(10000000))
	dialer, err := amqpextra.NewDialer(
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

	assertConnectionReady(t, dialer)

	require.True(t, rabbitmq.CloseConn(connName))

	assertConnectionUnready(t, dialer)

	assertConnectionReady(t, dialer)
}

func assertConnectionReady(t *testing.T, d *amqpextra.Dialer) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-d.NotifyReady():
	case <-timer.C:
		t.Fatal("dialer must be ready")
	}
}

func assertConnectionUnready(t *testing.T, d *amqpextra.Dialer) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-d.NotifyUnready():
	case <-timer.C:
		t.Fatal("dialer must be unready")
	}
}
