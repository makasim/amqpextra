package e2e_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDialerReconnectWhenClosedByClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	stateCh := make(chan amqpextra.State, 1)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithNotify(stateCh),
	)
	require.NoError(t, err)
	defer dialer.Close()

	assertDialerReady(t, stateCh)
	conn := <-dialer.ConnectionCh()

	go func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, conn.AMQPConnection().Close())
	}()
	assertDialerUnready(t, stateCh)

	assertDialerReady(t, stateCh)
}

func TestDialerReconnectWhenClosedByServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	rnum, err := rand.Int(rand.Reader, big.NewInt(10000000))
	require.NoError(t, err)
	stateCh := make(chan amqpextra.State, 1)

	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rnum)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithConnectionProperties(amqp.Table{
			"connection_name": connName,
		}),
		amqpextra.WithNotify(stateCh),
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

	assertDialerReady(t, stateCh)

	require.True(t, rabbitmq.CloseConn(connName))

	assertDialerUnready(t, stateCh)

	assertDialerReady(t, stateCh)
}

func assertDialerReady(t *testing.T, stateCh <-chan amqpextra.State) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case state := <-stateCh:
		assert.Nil(t, state.Unready)
		assert.NotNil(t, state.Ready)
	case <-timer.C:
		t.Fatal("dialer must be ready")
	}
}

func assertDialerUnready(t *testing.T, stateCh <-chan amqpextra.State) {
	timer := time.NewTimer(time.Millisecond * 2000)
	defer timer.Stop()

	select {
	case state := <-stateCh:
		assert.Nil(t, state.Ready)
		assert.NotNil(t, state.Unready)
	case <-timer.C:
		t.Fatal("dialer must be ready")
	}
}
