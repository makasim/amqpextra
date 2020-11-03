package e2e_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
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

	readyCh := make(chan struct{}, 1)
	unreadyCh := make(chan error, 2)

	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithNotify(readyCh, unreadyCh),
	)
	require.NoError(t, err)
	defer dialer.Close()

	assertConnectionReady(t, readyCh)
	conn := <-dialer.ConnectionCh()

	go func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, conn.AMQPConnection().Close())
	}()
	assertConnectionUnready(t, unreadyCh)

	assertConnectionReady(t, readyCh)
}

func TestDialerReconnectWhenClosedByServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	rnum, err := rand.Int(rand.Reader, big.NewInt(10000000))
	require.NoError(t, err)
	readyCh := make(chan struct{}, 1)
	unreadyCh := make(chan error, 1)
	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rnum)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": connName,
		}),
		amqpextra.WithNotify(readyCh, unreadyCh),
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

	assertConnectionReady(t, readyCh)

	require.True(t, rabbitmq.CloseConn(connName))

	assertConnectionUnready(t, unreadyCh)

	assertConnectionReady(t, readyCh)
}

func assertConnectionReady(t *testing.T, readyCh chan struct{}) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-readyCh:
	case <-timer.C:
		t.Fatal("dialer must be ready")
	}
}

func assertConnectionUnready(t *testing.T, unreadyCh chan error) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case <-unreadyCh:
	case <-timer.C:
		t.Fatal("dialer must be unready")
	}
}
