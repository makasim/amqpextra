package publisher_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"sync"

	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"go.uber.org/goleak"
)

func TestPublish(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := logger.NewTest()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
	connCh, closeCh := conn.ConnCh()
	p := publisher.NewBridge(connCh, closeCh, publisher.WithLogger(l))

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resultCh := make(chan error, 1)
			for i := 0; i < 10; i++ {
				p.Publish(publisher.Message{
					ResultCh: resultCh,
				})
				<-resultCh
			}

			time.Sleep(time.Millisecond * 100)
		}()
	}

	wg.Wait()
	p.Close()
	<-p.Closed()
	conn.Close()
	time.Sleep(time.Millisecond * 100)

	expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
	require.Equal(t, expected, l.Logs())
}
