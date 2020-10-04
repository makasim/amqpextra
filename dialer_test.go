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
func ExampleDialer_ConnectionCh() {
	dialer, err := amqpextra.NewDialer(amqpextra.WithURL("amqp://guest:guest@localhost:5672/%2f"))
	if err != nil {
		log.Fatal(err)
	}

	connCh := dialer.ConnectionCh()
	go func() {
	L1:
		for {
			select {
			case conn, ok := <-connCh:
				if !ok {
					// connection permanently closed
					return
				}

				ch, err := conn.AMQPConnection().Channel()
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
					case <-conn.NotifyLost():
						// connection is lost. let`s get new one
						continue L1
					case <-conn.NotifyClose():
						// connection is closed
						return
					}
				}
			}
		}
	}()

	time.Sleep(time.Second)
	dialer.Close()

	// Output:
}

func TestOptions(main *testing.T) {
	main.Run("NoURL", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.NewDialer()
		require.EqualError(t, err, "url(s) must be set")
	})

	main.Run("ZeroRetryPeriod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.NewDialer(
			amqpextra.WithURL("URL"),
			amqpextra.WithRetryPeriod(time.Duration(0)),
		)
		require.EqualError(t, err, "retryPeriod must be greater then zero")
	})

	main.Run("NegativeRetryPeriod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.NewDialer(
			amqpextra.WithURL("URL"),
			amqpextra.WithRetryPeriod(time.Duration(-1)),
		)
		require.EqualError(t, err, "retryPeriod must be greater then zero")
	})
}

func TestConnectState(main *testing.T) {
	main.Run("CloseWhileDialingErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, fmt.Errorf("dialing errored"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, amqp.ErrClosed.Error())

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: dialing errored
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("CloseWhileDialing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().Close().Return(nil)
		conn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
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

	main.Run("CloseByContextWhileDialing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().Close().Return(nil)
		conn.EXPECT().NotifyClose(any()).Return(closeCh)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, conn)),
			amqpextra.WithLogger(l),
			amqpextra.WithContext(ctx),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, amqp.ErrClosed.Error())

		assertReady(t, dialer)

		cancelFunc()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("NoConnWhileDialing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertNoConn(t, dialer.ConnectionCh())
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

	main.Run("CloseWhileWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.NewDialer(
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

	main.Run("CloseByContextWhileWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithLogger(l),
			amqpextra.WithContext(ctx),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, dialer, "the error")

		cancelFunc()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("NoConnWhileWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertNoConn(t, dialer.ConnectionCh())
		assertUnready(t, dialer, "the error")

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("ReadyAfterWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"), amqpConn)),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertNoConn(t, dialer.ConnectionCh())
		assertUnready(t, dialer, "the error")
		time.Sleep(time.Millisecond * 50)
		assertReady(t, dialer)

		conn := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn)
		assertConnNotLost(t, conn)

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

	main.Run("GetConnectionTimeout", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		dialer, err := amqpextra.NewDialer(
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

		dialer, err := amqpextra.NewDialer(
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

func TestConnectedState(main *testing.T) {
	main.Run("Ready", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)

		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, dialer)

		conn := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn)
		assertConnNotLost(t, conn)

		dialer.Close()
		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("ReconnectOnError", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn0 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn0.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn0.EXPECT().Close().Return(nil)

		closeCh1 := make(chan *amqp.Error, 1)
		amqpConn1 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn1.EXPECT().NotifyClose(any()).Return(closeCh1)
		amqpConn1.EXPECT().Close().Return(nil)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn0, amqpConn1)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, dialer)

		conn0 := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn0)
		assertConnNotLost(t, conn0)

		closeCh0 <- amqp.ErrClosed

		assertConnLost(t, conn0)
		assertConnNotClosed(t, conn0)

		assertReady(t, dialer)

		conn1 := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn1)
		assertConnNotLost(t, conn1)

		dialer.Close()
		assertConnClosed(t, conn1)
		assertClosed(t, dialer)
		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("ReconnectOnClose", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn0 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn0.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn0.EXPECT().Close().Return(nil)

		closeCh1 := make(chan *amqp.Error, 1)
		amqpConn1 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn1.EXPECT().NotifyClose(any()).Return(closeCh1)
		amqpConn1.EXPECT().Close().Return(nil)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn0, amqpConn1)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, dialer)

		conn0 := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn0)
		assertConnNotLost(t, conn0)

		close(closeCh0)

		assertConnLost(t, conn0)
		assertConnNotClosed(t, conn0)

		assertReady(t, dialer)

		conn1 := <-dialer.ConnectionCh()
		assertConnNotClosed(t, conn1)
		assertConnNotLost(t, conn1)

		dialer.Close()

		assertConnClosed(t, conn1)
		assertClosed(t, dialer)
		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("ConnectionCloseIgnoreErrClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn.EXPECT().Close().Return(amqp.ErrClosed)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, dialer)

		dialer.Close()
		assertClosed(t, dialer)
		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`, l.Logs())
	})

	main.Run("ConnectionCloseErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn.EXPECT().Close().Return(fmt.Errorf("connection closed errored"))

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, dialer)

		dialer.Close()
		assertClosed(t, dialer)
		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[ERROR] connection closed errored
[DEBUG] connection closed
`, l.Logs())
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

func assertNoConn(t *testing.T, connCh <-chan *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-connCh:
		t.Fatal("connection is not expected")
	case <-timer.C:
	}
}

func assertConnNotLost(t *testing.T, conn *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-conn.NotifyLost():
		t.Fatal("connection should not be lost")
	case <-timer.C:
	}
}

func assertConnNotClosed(t *testing.T, conn *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-conn.NotifyClose():
		t.Fatal("connection should not be closed")
	case <-timer.C:
	}
}

func assertConnLost(t *testing.T, conn *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-conn.NotifyLost():
	case <-timer.C:
		t.Fatal("wait connection lost timeout")
	}
}

func assertConnClosed(t *testing.T, conn *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-conn.NotifyClose():
	case <-timer.C:
		t.Fatal("wait connection closed timeout")
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
