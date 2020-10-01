package amqpextra_test

import (
	"log"

	"time"

	"testing"

	"fmt"

	"context"

	"github.com/golang/mock/gomock"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/mock_amqpextra"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// nolint:gosimple // the purpose of select case is to stress the connCh close case.
func ExampleDialer_NotifyReady() {
	conn, err := amqpextra.New(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))
	if err != nil {
		log.Fatal(err)
	}

	readyCh := conn.NotifyReady()
	go func() {
	L1:

		for {
			select {
			case ready, ok := <-readyCh:
				if !ok {
					// connection permanently closed
					return
				}

				ch, err := ready.AMQPConnection().Channel()
				if err != nil {
					return
				}

				ticker := time.NewTicker(time.Second * 5)
				for {
					select {
					case <-ticker.C:
						// do some stuff
						err := ch.Publish("", "a_queue", false, false, amqp.Publishing{
							Body: []byte("I've got some news!"),
						})
						if err != nil {
							log.Print(err)
						}
					case <-ready.NotifyClose():
						// connection is lost. let`s get new one
						continue L1
					}
				}
			}

		}
	}()

	time.Sleep(time.Second)
	conn.Close()

	// Output:
}

func TestUnready(main *testing.T) {
	main.Run("UnreadyWhileDialingErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, fmt.Errorf("connection timeout"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, amqp.ErrClosed.Error())

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: connection timeout
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("CloseWhileDialingSuccessfully", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().Close().Return(nil)
		conn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, conn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, amqp.ErrClosed.Error())

		assertReady(t, dialer)

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("UnreadyWhileWaitingRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, "the error")

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("UnreadyReadyWhileWaitingRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().Close().Return(nil)
		conn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"), conn)),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, "the error")

		assertReady(t, dialer)

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("GetConnectionContextTimeout", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*75, fmt.Errorf("the error"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancelFunc()
		_, err = dialer.Connection(ctx)
		require.EqualError(t, err, "context deadline exceeded")

		dialer.Close()
		assertClosed(t, dialer)
	})

	main.Run("GetConnectionDialerClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*75, fmt.Errorf("the error"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		dialer.Close()
		assertClosed(t, dialer)

		_, err = dialer.Connection(context.Background())
		require.EqualError(t, err, "connection closed")
	})
}

func TestOptions(main *testing.T) {
	main.Run("NoURL", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.New()
		require.EqualError(t, err, "url(s) must be set")
	})

	main.Run("ZeroRetryPeriod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.New(
			amqpextra.WithURL("URL"),
			amqpextra.WithRetryPeriod(time.Duration(0)),
		)
		require.EqualError(t, err, "retryPeriod must be greater then zero")
	})

	main.Run("NegativeRetryPeriod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.New(
			amqpextra.WithURL("URL"),
			amqpextra.WithRetryPeriod(time.Duration(-1)),
		)
		require.EqualError(t, err, "retryPeriod must be greater then zero")
	})
}

func TestClose(main *testing.T) {
	main.Run("CloseTwice", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*20, fmt.Errorf("the error"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		dialer.Close()
		dialer.Close()
	})

	main.Run("CloseByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		l := logger.NewTest()

		dialer, err := amqpextra.New(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*20, fmt.Errorf("the error"))),
			amqpextra.WithContext(ctx),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		cancelFunc()
		assertClosed(t, dialer)
	})
}

func assertUnready(t *testing.T, c *amqpextra.Dialer, errString string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case err, ok := <-c.NotifyUnready():
		if !ok {
			require.Equal(t, "permanently closed", errString)
			return
		}

		require.EqualError(t, err, errString)
	case <-timer.C:
		t.Fatal("dialer must be unready")
	}
}

func assertReady(t *testing.T, c *amqpextra.Dialer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case _, ok := <-c.NotifyReady():
		if !ok {
			t.Fatal("dialer notify ready closed")
		}
	case <-timer.C:
		t.Fatal("dialer must be ready")
	}
}

func assertClosed(t *testing.T, c *amqpextra.Dialer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.NotifyClosed():
	case <-timer.C:
		t.Fatal("dialer close timeout")
	}
}

func any() gomock.Matcher {
	return gomock.Any()
}

func amqpDialStub(conns ...interface{}) func(url string, config amqp.Config) (amqpextra.AMQPConnection, error) {
	index := 0
	return func(url string, config amqp.Config) (amqpextra.AMQPConnection, error) {
		if dur, ok := conns[index].(time.Duration); ok {
			time.Sleep(dur)
			index++
		}

		switch curr := conns[index].(type) {
		case amqpextra.AMQPConnection:
			index++
			return curr, nil
		case error:
			index++
			return nil, curr
		default:
			panic(fmt.Sprintf("unexpected type given: %T", conns[index]))
		}
	}
}
