package e2e_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/e2e_test/helper/rabbitmq"
	"github.com/makasim/amqpextra/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPublishWhileConnectionClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	rnum, err := rand.Int(rand.Reader, big.NewInt(10000000))
	require.NoError(t, err)
	connName := fmt.Sprintf("amqpextra-test-%d-%d", time.Now().UnixNano(), rnum)
	stateCh := make(chan amqpextra.State, 1)
	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
		amqpextra.WithNotify(stateCh),
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
	p, err := dialer.Publisher()
	require.NoError(t, err)
	assertDialerReady(t, stateCh)

	count := 0
	errorCount := 0
	for i := 0; i < 1000; i++ {
		if i == 300 {
			time.Sleep(time.Millisecond * 100)
			require.True(t, rabbitmq.CloseConn(connName))
		}

		res := p.Publish(publisher.Message{})

		if res == nil {
			count++
		} else {
			errorCount++
		}
	}

	assert.GreaterOrEqual(t, count, 995)

	dialer.Close()
	<-p.NotifyClosed()
}

func TestPublishConfirms(t *testing.T) {
	defer goleak.VerifyNone(t)

	dialer, err := amqpextra.NewDialer(
		amqpextra.WithURL("amqp://guest:guest@rabbitmq:5672/amqpextra"),
	)
	require.NoError(t, err)
	defer dialer.Close()

	pub, err := dialer.Publisher(
		publisher.WithConfirmation(10),
	)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		msg := publisher.Message{}
		err = pub.Publish(msg)
		require.NoError(t, err, "result must be nil")
	}

	dialer.Close()
	<-pub.NotifyClosed()
}
