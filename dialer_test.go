package amqpextra_test

import (
	"errors"
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
					// connection is permanently closed
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

	main.Run("ErrorEmptyURl", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_, err := amqpextra.NewDialer(
			amqpextra.WithURL("", "url"),
		)
		require.EqualError(t, err, "url(s) must be not empty")
	})
}

func TestConnectState(main *testing.T) {
	main.Run("CloseWhileDialingErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		l := logger.NewTest()
		stateCh := make(chan amqpextra.State, 2)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*50, fmt.Errorf("dialing errored"))),
			amqpextra.WithLogger(l),
			amqpextra.WithNotify(stateCh),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 10)

		assertUnready(t, stateCh, "dialing errored")

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

		stateCh := make(chan amqpextra.State, 2)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().Close().Return(nil)
		conn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, conn)),
			amqpextra.WithLogger(l),
			amqpextra.WithNotify(stateCh),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		assertReady(t, stateCh)

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
		stateCh := make(chan amqpextra.State, 2)

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
			amqpextra.WithNotify(stateCh),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertReady(t, stateCh)

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
		stateCh := make(chan amqpextra.State, 2)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(time.Millisecond*150, amqpConn)),
			amqpextra.WithLogger(l),
			amqpextra.WithNotify(stateCh),
		)
		require.NoError(t, err)

		assertNoConn(t, dialer.ConnectionCh())
		assertReady(t, stateCh)

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

		stateCh := make(chan amqpextra.State, 2)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithNotify(stateCh),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, stateCh, "the error")
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

		stateCh := make(chan amqpextra.State, 2)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithNotify(stateCh),
			amqpextra.WithLogger(l),
			amqpextra.WithContext(ctx),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)
		assertUnready(t, stateCh, "the error")

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

		stateCh := make(chan amqpextra.State, 2)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"))),
			amqpextra.WithRetryPeriod(time.Millisecond*150),
			amqpextra.WithNotify(stateCh),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertUnready(t, stateCh, "the error")
		assertNoConn(t, dialer.ConnectionCh())

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
		stateCh := make(chan amqpextra.State, 2)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithNotify(stateCh),
			amqpextra.WithAMQPDial(amqpDialStub(fmt.Errorf("the error"), amqpConn)),
			amqpextra.WithRetryPeriod(time.Millisecond*100),
			amqpextra.WithLogger(l),
		)

		require.NoError(t, err)
		defer dialer.Close()

		assertNoConn(t, dialer.ConnectionCh())
		assertUnready(t, stateCh, "the error")

		assertReady(t, stateCh)

		conn := <-dialer.ConnectionCh()
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

	main.Run("UrlOrderInDial", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		urls := []string{
			"the.first.url",
			"the.second.url",
			"the.last.url",
		}

		index := 0
		dialSub := func(url string, config amqp.Config) (amqpextra.AMQPConnection, error) {
			require.Equal(t, url,
				[]string{
					"the.first.url",
					"the.second.url",
					"the.last.url",
					"the.first.url",
				}[index])
			index++
			return nil, errors.New("the error")
		}

		d, err := amqpextra.NewDialer(
			amqpextra.WithRetryPeriod(time.Millisecond*20),
			amqpextra.WithURL(urls...),
			amqpextra.WithAMQPDial(dialSub),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 70)

		d.Close()
	})
}

func TestNotify(main *testing.T) {
	main.Run("PanicIfStateChUnbuffered", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan amqpextra.State)

		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)

		require.PanicsWithValue(t, "state chan is unbuffered", func() {
			d, _ := amqpextra.NewDialer(
				amqpextra.WithAMQPDial(
					amqpDialStub(amqpConn),
				),
			)
			d.Notify(stateCh)
		})
	})

	main.Run("NoChangeStateWhileDial", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		stateCh := make(chan amqpextra.State, 1)
		l := logger.NewTest()
		d, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithLogger(l),
			amqpextra.WithAMQPDial(
				amqpDialStub(time.Millisecond*100, fmt.Errorf("the error")),
			),
		)
		require.NoError(t, err)

		newStateCh := d.Notify(make(chan amqpextra.State, 1))
		assertUnready(t, newStateCh, amqp.ErrClosed.Error())
		assertNoStateChanged(t, stateCh)
		time.Sleep(time.Millisecond * 100)
		assertUnready(t, newStateCh, "the error")

		d.Close()
		assertClosed(t, d)

		expected := `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] connection closed
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ReadyIfConnected", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		stateCh := make(chan amqpextra.State, 2)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().NotifyClose(any()).AnyTimes()
		conn.EXPECT().Close().AnyTimes()

		d, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithLogger(l),
			amqpextra.WithAMQPDial(
				amqpDialStub(conn),
			),
		)
		require.NoError(t, err)
		defer d.Close()

		assertNoStateChanged(t, stateCh)
		newStateCh := d.Notify(make(chan amqpextra.State, 1))
		assertReady(t, newStateCh)

		d.Close()
		assertClosed(t, d)

		expected := `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection closed
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyWhileWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		stateCh := make(chan amqpextra.State, 1)

		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().NotifyClose(any()).AnyTimes()
		conn.EXPECT().Close().AnyTimes()

		d, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithLogger(l),
			amqpextra.WithAMQPDial(
				amqpDialStub(time.Millisecond*10, errors.New("the error")),
			),
			amqpextra.WithRetryPeriod(time.Millisecond*100),
		)
		require.NoError(t, err)

		newStateCh := d.Notify(stateCh)
		assertUnready(t, newStateCh, amqp.ErrClosed.Error())

		assertUnready(t, newStateCh, "the error")

		d.Close()
		assertClosed(t, d)

		expected := `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection unready: the error
[DEBUG] connection closed
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyAfterClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		conn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		conn.EXPECT().NotifyClose(any()).AnyTimes()
		conn.EXPECT().Close().AnyTimes()

		d, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithLogger(l),
			amqpextra.WithAMQPDial(
				amqpDialStub(time.Millisecond*10, conn),
			),
		)
		require.NoError(t, err)

		newStateCh := d.Notify(make(chan amqpextra.State, 2))

		d.Close()
		assertClosed(t, d)
		assertUnready(t, newStateCh, amqp.ErrClosed.Error())

		expected := `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection closed
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestConnectedState(main *testing.T) {
	main.Run("Ready", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()

		closeCh := make(chan *amqp.Error)
		stateCh := make(chan amqpextra.State, 2)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().Close().Return(nil)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithNotify(stateCh),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, stateCh)

		conn := <-dialer.ConnectionCh()
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
		stateCh := make(chan amqpextra.State, 2)
		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn0 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn0.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn0.EXPECT().Close().Return(nil)

		closeCh1 := make(chan *amqp.Error, 1)
		amqpConn1 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn1.EXPECT().NotifyClose(any()).Return(closeCh1)
		amqpConn1.EXPECT().Close().Return(nil)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithNotify(stateCh),
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn0, amqpConn1)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, stateCh)

		conn0 := <-dialer.ConnectionCh()
		assertConnNotLost(t, conn0)

		closeCh0 <- amqp.ErrClosed
		assertConnLost(t, conn0)
		assertUnready(t, stateCh, amqp.ErrClosed.Error())

		conn1 := <-dialer.ConnectionCh()
		assertConnNotLost(t, conn1)
		assertReady(t, stateCh)

		dialer.Close()
		assertClosed(t, dialer)
		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection unready: Exception (504) Reason: "channel/connection is not open"
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
		stateCh := make(chan amqpextra.State, 1)
		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn0 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn0.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn0.EXPECT().Close().Return(nil)

		closeCh1 := make(chan *amqp.Error, 1)

		amqpConn1 := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn1.EXPECT().NotifyClose(any()).Return(closeCh1)
		amqpConn1.EXPECT().Close().Return(nil)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithNotify(stateCh),
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn0, amqpConn1)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		conn0 := <-dialer.ConnectionCh()
		assertConnNotLost(t, conn0)
		assertReady(t, stateCh)
		assertNoStateChanged(t, stateCh)

		close(closeCh0)
		assertConnLost(t, conn0)

		assertUnready(t, stateCh, amqp.ErrClosed.Error())

		conn1 := <-dialer.ConnectionCh()

		assertConnNotLost(t, conn1)
		assertReady(t, stateCh)

		dialer.Close()

		assertClosed(t, dialer)

		assert.Equal(t, `[DEBUG] connection unready
[DEBUG] dialing
[DEBUG] connection ready
[DEBUG] connection unready: Exception (504) Reason: "channel/connection is not open"
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

		stateCh := make(chan amqpextra.State, 2)
		closeCh0 := make(chan *amqp.Error, 1)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn.EXPECT().Close().Return(amqp.ErrClosed)

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithNotify(stateCh),
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, stateCh)

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

		stateCh := make(chan amqpextra.State, 2)
		amqpConn := mock_amqpextra.NewMockAMQPConnection(ctrl)
		amqpConn.EXPECT().NotifyClose(any()).Return(closeCh0)
		amqpConn.EXPECT().Close().Return(fmt.Errorf("connection closed errored"))

		dialer, err := amqpextra.NewDialer(
			amqpextra.WithNotify(stateCh),
			amqpextra.WithURL("amqp://rabbitmq.host"),
			amqpextra.WithAMQPDial(amqpDialStub(amqpConn)),
			amqpextra.WithLogger(l),
		)
		require.NoError(t, err)

		assertReady(t, stateCh)

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

func assertUnready(t *testing.T, stateCh <-chan amqpextra.State, errString string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state := <-stateCh:

		require.Nil(t, state.Ready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.EqualError(t, state.Unready.Err, errString)
	case <-timer.C:
		t.Fatal("consumer must be unready")
	}
}

func assertReady(t *testing.T, stateCh <-chan amqpextra.State) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state := <-stateCh:
		require.Nil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Ready, fmt.Sprintf("%+v", state))
	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}

func assertNoStateChanged(t *testing.T, stateCh <-chan amqpextra.State) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()
	select {
	case <-stateCh:
		t.Fatal("state change is not expected")
	case <-timer.C:
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

func assertConnLost(t *testing.T, conn *amqpextra.Connection) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-conn.NotifyLost():
	case <-timer.C:
		t.Fatal("wait connection lost timeout")
	}
}

func any() gomock.Matcher {
	return gomock.Any()
}

func amqpDialStub(conns ...interface{}) func(url string, config amqp.Config) (amqpextra.AMQPConnection, error) {
	index := 0
	return func(url string, config amqp.Config) (amqpextra.AMQPConnection, error) {
		if index == len(conns) {
			panic(fmt.Sprintf("dial stub must be called not more than %d times", len(conns)))
		}
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
