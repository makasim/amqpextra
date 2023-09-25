package publisher_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/makasim/amqpextra/publisher/mock_publisher"
)

func TestNotify(main *testing.T) {
	main.Run("PanicIfStateChUnbuffered", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		stateCh := make(chan publisher.State)

		_, _, p := newPublisher()
		defer p.Close()

		require.PanicsWithValue(t, "state chan is unbuffered", func() {
			p.Notify(stateCh)
		})
	})

	main.Run("UnreadyWhileInit", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		conn, l, p := newPublisher(
			publisher.WithInitFunc(
				func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
					time.Sleep(time.Millisecond * 50)
					return nil, errors.New("the error")
				}),
		)
		defer p.Close()

		newStateCh := p.Notify(make(chan publisher.State, 1))

		conn <- publisher.NewConnection(amqpConn, nil)

		assertUnready(t, newStateCh, amqp.ErrClosed.Error())
		assertUnready(t, newStateCh, "the error")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[ERROR] init func: the error
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ReadyIfConnected", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.EXPECT().NotifyClose(gomock.Any()).Times(1)
		ch.EXPECT().NotifyFlow(gomock.Any()).Times(1)
		ch.EXPECT().Close().AnyTimes()

		conn, l, p := newPublisher(
			publisher.WithInitFunc(
				func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
					return ch, nil
				}),
		)
		defer p.Close()

		newStateCh := p.Notify(stateCh)

		assertUnready(t, stateCh, amqp.ErrClosed.Error())

		conn <- publisher.NewConnection(amqpConn, nil)

		time.Sleep(time.Millisecond * 10)

		assertReady(t, newStateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`

		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyWhileWaitRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		conn, l, p := newPublisher(
			publisher.WithInitFunc(func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
			publisher.WithRestartSleep(time.Millisecond*400),
		)
		defer p.Close()

		newStateCh := p.Notify(stateCh)

		assertUnready(t, newStateCh, amqp.ErrClosed.Error())

		conn <- publisher.NewConnection(amqpConn, nil)

		time.Sleep(time.Millisecond * 100)

		assertUnready(t, newStateCh, "the error")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[ERROR] init func: the error
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyWhilePaused", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)

		var chFlowCh chan bool

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			AnyTimes()

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
		)
		defer p.Close()

		newStateCh := p.Notify(stateCh)
		assertUnready(t, newStateCh, amqp.ErrClosed.Error())

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, newStateCh)

		chFlowCh <- false

		assertUnready(t, stateCh, "publisher flow paused")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[DEBUG] publisher unready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyAfterClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			AnyTimes()
		ch.
			EXPECT().
			Close().
			AnyTimes()

		_, _, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch, "the error")),
		)

		newStateCh := p.Notify(stateCh)
		p.Close()

		assertUnready(t, newStateCh, amqp.ErrClosed.Error())
		assertClosed(t, p)
	})
}

func TestReconnection(main *testing.T) {
	main.Run("InitFuncRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			AnyTimes()
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		retry := 2
		stateCh := make(chan publisher.State, 1)

		connCh, l, p := newPublisher(
			publisher.WithNotify(stateCh),
			publisher.WithInitFunc(func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
				if retry > 0 {
					retry--
					return nil, fmt.Errorf("init func errored: %d", retry)
				}
				return ch, nil
			}),
			publisher.WithRestartSleep(time.Millisecond*10),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, nil)
		assertUnready(t, stateCh, fmt.Sprintf("init func errored: %d", 1))
		connCh <- publisher.NewConnection(amqpConn, nil)
		assertUnready(t, stateCh, fmt.Sprintf("init func errored: %d", 0))
		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[ERROR] init func: init func errored: 1
[DEBUG] publisher unready
[ERROR] init func: init func errored: 0
[DEBUG] publisher unready
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("UnreadyWhileInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)
		connCh, l, p := newPublisher(
			publisher.WithNotify(stateCh),
			publisher.WithInitFunc(func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
			publisher.WithRestartSleep(time.Millisecond*100),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, nil)

		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[ERROR] init func: the error
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConnectionLost", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		newAMQPConn := mock_publisher.NewMockAMQPConnection(ctrl)
		stateCh := make(chan publisher.State, 2)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch, newCh)),
			publisher.WithNotify(stateCh))
		defer p.Close()

		closeCh := make(chan struct{})
		conn := publisher.NewConnection(amqpConn, closeCh)
		connCh <- conn
		assertReady(t, stateCh)

		close(closeCh)
		assertUnready(t, stateCh, amqp.ErrClosed.Error())

		connCh <- publisher.NewConnection(newAMQPConn, nil)

		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher unready
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConnectionPermanentlyClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		closeCh := make(chan struct{})
		conn := publisher.NewConnection(amqpConn, closeCh)

		assertNoStateChanged(t, stateCh)

		connCh <- conn
		assertReady(t, stateCh)

		close(closeCh)
		close(connCh)
		assertUnready(t, stateCh, amqp.ErrClosed.Error())

		time.Sleep(time.Millisecond * 50)
		p.Close()
		assertClosed(t, p)
		assertNoStateChanged(t, stateCh)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher unready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ChannelClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		chCloseCh := make(chan *amqp.Error)
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch, newCh)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		chCloseCh <- amqp.ErrClosed

		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] channel closed
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ChannelClosedAndInitFuncErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		chCloseCh := make(chan *amqp.Error)
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(chCloseChStub(chCloseCh)).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		newAMQPConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh, l, p := newPublisher(
			publisher.WithRestartSleep(time.Millisecond),
			publisher.WithInitFunc(initFuncStub(ch, fmt.Errorf("init func errored"), newCh)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		chCloseCh <- amqp.ErrClosed
		assertUnready(t, stateCh, "init func errored")

		connCh <- publisher.NewConnection(newAMQPConn, nil)
		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] channel closed
[ERROR] init func: init func errored
[DEBUG] publisher unready
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestUnreadyReady(main *testing.T) {
	main.Run("ClosedPublisherUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		stateCh := make(chan publisher.State, 2)

		ctrl := gomock.NewController(t)
		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		conn := mock_publisher.NewMockAMQPConnection(ctrl)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			AnyTimes()
		ch.
			EXPECT().
			Close().
			AnyTimes()

		connCh, _, p := newPublisher(
			publisher.WithNotify(stateCh),
			publisher.WithInitFunc(initFuncStub(fmt.Errorf("the error"))),
		)
		defer p.Close()
		connCh <- publisher.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 10)
		p.Close()
		assertUnready(t, stateCh, "the error")
		assertClosed(t, p)
	})

	main.Run("PublishWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)
		stateCh := make(chan publisher.State, 2)

		p, err := publisher.New(
			connCh,
			publisher.WithLogger(l),
			publisher.WithNotify(stateCh),
		)
		require.NoError(t, err)
		defer p.Close()

		resultCh := p.Go(publisher.Message{
			ErrOnUnready: true,
			Publishing:   amqp.Publishing{},
			ResultCh:     nil,
		})

		err = waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publisher not ready")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)
		stateCh := make(chan publisher.State, 1)

		p, err := publisher.New(
			connCh,
			publisher.WithLogger(l),
			publisher.WithNotify(stateCh),
		)
		require.NoError(t, err)
		defer p.Close()

		resultCh := p.Go(publisher.Message{
			ErrOnUnready: true,
			Publishing:   amqp.Publishing{},
			ResultCh:     make(chan error, 1),
		})

		err = waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publisher not ready")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithUnbufferedResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)

		p, err := publisher.New(connCh, publisher.WithLogger(l))
		require.NoError(t, err)
		defer p.Close()

		assert.PanicsWithValue(t, "amqpextra: resultCh channel is unbuffered", func() {
			p.Go(publisher.Message{
				Context:    context.Background(),
				Publishing: amqp.Publishing{},
				ResultCh:   make(chan error),
			})
		})

		p.Close()
		assertClosed(t, p)
	})

	main.Run("PublishMessageContextClosedWhileWaitingReady", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		go func() {
			time.Sleep(time.Millisecond * 400)
			amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

			connCh <- publisher.NewConnection(amqpConn, nil)
		}()

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond * 200)
			cancelFunc()
		}()
		p.Go(publisher.Message{
			Context:    msgCtx,
			Publishing: amqp.Publishing{},
		})

		time.Sleep(time.Millisecond * 100)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestReadyPublisher(main *testing.T) {
	main.Run("PublishExpectedMessage", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				require.Equal(t, "theExchange", exchange)
				require.Equal(t, "theKey", key)
				require.True(t, mandatory)
				require.True(t, immediate)
				require.Equal(t, amqp.Publishing{
					Headers: amqp.Table{
						"fooHeader": "fooHeaderVal",
					},
					ContentType:     "theContentType",
					ContentEncoding: "theContentEncoding",
					DeliveryMode:    3,
					Priority:        4,
					CorrelationId:   "theCorrId",
					ReplyTo:         "theReplyTo",
					Expiration:      "theExpiration",
					MessageId:       "theMessageId",
					Timestamp:       time.Unix(123, 567),
					Type:            "theType",
					UserId:          "theUserId",
					AppId:           "theApp",
					Body:            []byte(`theBody`),
				}, msg)

				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)
		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		resultCh := p.Go(publisher.Message{
			Exchange:  "theExchange",
			Key:       "theKey",
			Mandatory: true,
			Immediate: true,
			Publishing: amqp.Publishing{
				Headers: amqp.Table{
					"fooHeader": "fooHeaderVal",
				},
				ContentType:     "theContentType",
				ContentEncoding: "theContentEncoding",
				DeliveryMode:    3,
				Priority:        4,
				CorrelationId:   "theCorrId",
				ReplyTo:         "theReplyTo",
				Expiration:      "theExpiration",
				MessageId:       "theMessageId",
				Timestamp:       time.Unix(123, 567),
				Type:            "theType",
				UserId:          "theUserId",
				AppId:           "theApp",
				Body:            []byte(`theBody`),
			},
		})

		err := waitResult(resultCh, time.Millisecond*100)
		require.NoError(t, err)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishErroredWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("publish errored")).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)
		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		resultCh := p.Go(publisher.Message{
			Publishing: amqp.Publishing{},
		})

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publish errored")

		time.Sleep(time.Millisecond * 100)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishErroredWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("publish errored")).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		resultCh := p.Go(publisher.Message{
			ResultCh:   make(chan error, 1),
			Publishing: amqp.Publishing{},
		})

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publish errored")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishMessageContextClosedBeforePublish", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			MaxTimes(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			MaxTimes(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			AnyTimes()

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		resultCh := p.Go(publisher.Message{
			Context:    msgCtx,
			Publishing: amqp.Publishing{},
		})

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "message: context canceled")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWaitForReady", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		go func() {
			<-time.NewTimer(time.Second).C

			connCh <- publisher.NewConnection(amqpConn, nil)
		}()

		before := time.Now().UnixNano()
		resultCh := p.Go(publisher.Message{
			Publishing: amqp.Publishing{},
			ResultCh:   make(chan error, 1),
		})

		assertReady(t, stateCh)

		err := waitResult(resultCh, time.Millisecond*1300)
		require.NoError(t, err)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
		after := time.Now().UnixNano()
		require.GreaterOrEqual(t, after-before, int64(900000000))
		require.LessOrEqual(t, after-before, int64(1100000000))
	})
}

func TestClosedPublisher(main *testing.T) {
	main.Run("PublishWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)

		p, err := publisher.New(connCh, publisher.WithLogger(l))
		require.NoError(t, err)

		p.Close()
		assertClosed(t, p)

		resultCh := p.Go(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
		})

		err = waitResult(resultCh, time.Millisecond*1300)
		require.EqualError(t, err, "publisher stopped")

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)
		stateCh := make(chan publisher.State, 2)

		p, err := publisher.New(
			connCh,
			publisher.WithLogger(l),
			publisher.WithNotify(stateCh),
		)
		require.NoError(t, err)

		p.Close()
		assertClosed(t, p)

		resultCh := p.Go(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
			ResultCh:   make(chan error, 1),
		})

		assertNoStateChanged(t, stateCh)

		err = waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, `publisher stopped`)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestClose(main *testing.T) {
	main.Run("CloseTwice", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)
		stateCh := make(chan publisher.State, 2)

		p, err := publisher.New(connCh,
			publisher.WithLogger(l),
			publisher.WithNotify(stateCh),
		)
		require.NoError(t, err)
		defer p.Close()

		p.Close()
		p.Close()
		assertClosed(t, p)
	})

	main.Run("CloseNoStateChangeByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan *publisher.Connection)
		stateCh := make(chan publisher.State, 2)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		p, err := publisher.New(connCh,
			publisher.WithContext(ctx),
			publisher.WithLogger(l),
			publisher.WithNotify(stateCh),
		)
		require.NoError(t, err)
		defer p.Close()

		assertNoStateChanged(t, stateCh)

		cancelFunc()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseReadyByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithNotify(stateCh),
			publisher.WithContext(ctx),
			publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		cancelFunc()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(fmt.Errorf("channel close errored")).
			Times(1)

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher: channel close: channel close errored
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseIgnoreClosedError", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestConcurrency(main *testing.T) {
	main.Run("CloseWhilePublishing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < 10; i++ {
					<-p.Go(publisher.Message{})
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseConnectionWhilePublishing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(10).
			MaxTimes(40)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		newAMQPConn := mock_publisher.NewMockAMQPConnection(ctrl)
		stateCh := make(chan publisher.State, 2)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(60).
			MaxTimes(90)
		newCh.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch, newCh)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := publisher.NewConnection(amqpConn, closeCh)
		connCh <- conn

		assertReady(t, stateCh)

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < 10; i++ {
					<-p.Go(publisher.Message{})
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		close(closeCh)

		connCh <- publisher.NewConnection(newAMQPConn, nil)

		time.Sleep(time.Millisecond * 900)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] publisher unready
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseChannelWhilePublishing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		chCloseCh := make(chan *amqp.Error)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(chCloseChStub(chCloseCh)).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(60).
			MaxTimes(80)
		newCh.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch, newCh)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < 10; i++ {
					<-p.Go(publisher.Message{})
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		chCloseCh <- amqp.ErrClosed

		time.Sleep(time.Millisecond * 900)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] channel closed
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestFlowControl(main *testing.T) {
	main.Run("FlowPausedResumedWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var chFlowCh chan bool
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(3)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		resultCh := make(chan error, 1)

		p.Go(publisher.Message{ResultCh: resultCh})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		chFlowCh <- false
		assertUnready(t, stateCh, "publisher flow paused")

		go p.Go(publisher.Message{ResultCh: resultCh})
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "wait result timeout")

		chFlowCh <- true
		assertReady(t, stateCh)

		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		p.Go(publisher.Message{ResultCh: resultCh})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[INFO] publisher flow resumed
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("FlowPausedResumedWithResultChannelErrOnUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var chFlowCh chan bool
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(2)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		resultCh := make(chan error, 1)

		p.Go(publisher.Message{
			ResultCh:     resultCh,
			ErrOnUnready: true,
		})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		chFlowCh <- false
		assertUnready(t, stateCh, "publisher flow paused")

		go p.Go(publisher.Message{
			ResultCh:     resultCh,
			ErrOnUnready: true,
		})
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "publisher not ready")

		chFlowCh <- true
		assertReady(t, stateCh)

		p.Go(publisher.Message{
			ResultCh:     resultCh,
			ErrOnUnready: true,
		})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[INFO] publisher flow resumed
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("FlowPausedResumedNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var chFlowCh chan bool
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(3)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertReady(t, stateCh)

		p.Go(publisher.Message{})

		chFlowCh <- false
		assertUnready(t, stateCh, "publisher flow paused")

		waitResult := make(chan struct{})
		go func() {
			defer close(waitResult)
			p.Go(publisher.Message{})
		}()

		chFlowCh <- true

		assertReady(t, stateCh)

		<-waitResult

		p.Go(publisher.Message{})

		p.Close()
		assertClosed(t, p)

		logs := l.Logs()
		require.Contains(t, logs, `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[INFO] publisher flow resumed
`)

		require.Contains(t, logs, `[DEBUG] publisher stopped`)
	})

	main.Run("ClosedWhileFlowPaused", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		chFlowCh := make(chan bool)
		// chanBuff ?
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			Return(chFlowCh).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		chFlowCh <- false

		assertUnready(t, stateCh, "publisher flow paused")

		p.Close()
		assertClosed(t, p)

		logs := l.Logs()
		require.Equal(t, `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[DEBUG] publisher unready
[DEBUG] publisher stopped
`, logs)
	})

	main.Run("ChannelClosedWhileFlowPaused", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var chFlowCh chan bool
		var chCloseCh chan *amqp.Error
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(2)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(2)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(
			publisher.WithRestartSleep(time.Millisecond),
			publisher.WithInitFunc(initFuncStub(ch, ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		chFlowCh <- false
		assertUnready(t, stateCh, "publisher flow paused")

		chCloseCh <- amqp.ErrClosed
		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		logs := l.Logs()
		require.Equal(t, `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[DEBUG] channel closed
[DEBUG] publisher ready
[DEBUG] publisher stopped
`, logs)

		require.Contains(t, logs, ``)
	})

	main.Run("ConnClosedWhileFlowPaused", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var chFlowCh chan bool
		stateCh := make(chan publisher.State, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(notifyCloseStub()).
			Times(2)
		ch.
			EXPECT().
			NotifyFlow(gomock.Any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(2)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(2)

		connCh, l, p := newPublisher(
			publisher.WithRestartSleep(time.Millisecond),
			publisher.WithInitFunc(initFuncStub(ch, ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := publisher.NewConnection(amqpConn, closeCh)
		connCh <- conn
		assertReady(t, stateCh)

		chFlowCh <- false

		assertUnready(t, stateCh, "publisher flow paused")

		close(closeCh)
		assertUnready(t, stateCh, amqp.ErrClosed.Error())
		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		p.Close()
		assertClosed(t, p)

		logs := l.Logs()
		require.Equal(t, `[DEBUG] publisher starting
[DEBUG] publisher ready
[WARN] publisher flow paused
[DEBUG] publisher unready
[DEBUG] publisher ready
[DEBUG] publisher stopped
`, logs)

		require.Contains(t, logs, ``)
	})
}

func TestPublisherConfirms(main *testing.T) {
	main.Run("ErrorIfBufferLessThanOne", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		connCh := make(chan *publisher.Connection)
		_, err := publisher.New(connCh, publisher.WithConfirmation(0))
		require.EqualError(t, err, "confirmation buffer size must be greater than 0")
	})

	main.Run("WaitRetryIfConfirmErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Confirm(false).Return(fmt.Errorf("the error")).Times(1)
		ch.EXPECT().Close().Times(1)
		defer ch.Close()

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithNotify(stateCh),
			publisher.WithRestartSleep(time.Millisecond*50),
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithConfirmation(2),
		)

		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- publisher.NewConnection(amqpConn, nil)

		assertUnready(t, stateCh, "the error")
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("AckConfirmTwoMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		confirmationCh := make(chan amqp.Confirmation, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			NotifyPublish(gomock.Any()).
			DoAndReturn(confirmationChStub(confirmationCh)).
			Times(1)
		ch.EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(2)
		ch.EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()

		ch.EXPECT().NotifyFlow(gomock.Any()).AnyTimes()
		ch.EXPECT().Confirm(false).Return(nil)
		ch.EXPECT().Close().AnyTimes()

		stateCh := make(chan publisher.State, 2)

		connCh, l, p := newPublisher(
			publisher.WithConfirmation(2),
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, nil)
		assertReady(t, stateCh)

		resultCh := make(chan error, 2)
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})

		confirmationCh <- amqp.Confirmation{Ack: true}
		confirmationCh <- amqp.Confirmation{Ack: true}

		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		close(confirmationCh)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] handle confirmation ready
[DEBUG] handle confirmation started
[DEBUG] handle confirmation stopped
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("NackConfirmTwoMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		confirmationCh := make(chan amqp.Confirmation, 2)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			NotifyPublish(gomock.Any()).
			DoAndReturn(confirmationChStub(confirmationCh)).
			Times(1)
		ch.EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(2)
		ch.EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()

		ch.EXPECT().NotifyFlow(gomock.Any()).AnyTimes()
		ch.EXPECT().Confirm(false).Return(nil)
		ch.EXPECT().Close().AnyTimes()

		stateCh := make(chan publisher.State, 2)
		connCloseCh := make(chan struct{})

		connCh, l, p := newPublisher(
			publisher.WithConfirmation(2),
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, connCloseCh)
		assertReady(t, stateCh)

		resultCh := make(chan error, 2)
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})

		confirmationCh <- amqp.Confirmation{Ack: false}
		confirmationCh <- amqp.Confirmation{Ack: false}

		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "confirmation: nack")
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "confirmation: nack")

		close(confirmationCh)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] handle confirmation ready
[DEBUG] handle confirmation started
[DEBUG] handle confirmation stopped
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConfirmationWhileConnectionClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		confirmationCh := make(chan amqp.Confirmation, 4)

		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			NotifyPublish(gomock.Any()).
			DoAndReturn(confirmationChStub(confirmationCh)).
			Times(1)
		ch.EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(4)
		ch.EXPECT().
			NotifyClose(gomock.Any()).
			AnyTimes()

		ch.EXPECT().NotifyFlow(gomock.Any()).AnyTimes()
		ch.EXPECT().Confirm(false).Return(nil)
		ch.EXPECT().Close().AnyTimes()

		stateCh := make(chan publisher.State, 2)
		connCloseCh := make(chan struct{})

		connCh, l, p := newPublisher(
			publisher.WithConfirmation(4),
			publisher.WithInitFunc(initFuncStub(ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, connCloseCh)
		assertReady(t, stateCh)

		resultCh := make(chan error, 4)
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})

		confirmationCh <- amqp.Confirmation{Ack: true}
		confirmationCh <- amqp.Confirmation{Ack: true}

		time.Sleep(time.Millisecond * 100)
		close(connCloseCh)
		close(confirmationCh)

		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), amqp.ErrClosed.Error())
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), amqp.ErrClosed.Error())

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] handle confirmation ready
[DEBUG] handle confirmation started
[DEBUG] handle confirmation stopped
[DEBUG] publisher unready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConfirmationWhileChannelClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		confirmationCh := make(chan amqp.Confirmation, 4)
		chCloseCh := make(chan *amqp.Error, 1)
		ch := mock_publisher.NewMockAMQPChannel(ctrl)

		secondConfirmationCh := make(chan amqp.Confirmation, 4)
		secondChCloseCh := make(chan *amqp.Error, 1)

		ch.EXPECT().
			NotifyPublish(gomock.Any()).
			DoAndReturn(confirmationChStub(confirmationCh, secondConfirmationCh)).
			Times(2)
		ch.EXPECT().
			PublishWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(4)
		ch.EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(chCloseChStub(chCloseCh, secondChCloseCh)).
			AnyTimes()

		ch.EXPECT().NotifyFlow(gomock.Any()).AnyTimes()
		ch.EXPECT().Confirm(false).Return(nil).AnyTimes()
		ch.EXPECT().Close().AnyTimes()

		stateCh := make(chan publisher.State, 2)
		connCloseCh := make(chan struct{})

		connCh, l, p := newPublisher(
			publisher.WithConfirmation(4),
			publisher.WithInitFunc(initFuncStub(ch, ch)),
			publisher.WithNotify(stateCh),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- publisher.NewConnection(amqpConn, connCloseCh)
		assertReady(t, stateCh)

		resultCh := make(chan error, 2)
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})
		p.Go(publisher.Message{ResultCh: resultCh})

		confirmationCh <- amqp.Confirmation{Ack: true}
		confirmationCh <- amqp.Confirmation{Ack: true}

		time.Sleep(time.Millisecond * 100)

		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		require.NoError(t, waitResult(resultCh, time.Millisecond*100))
		close(chCloseCh)
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), amqp.ErrClosed.Error())
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), amqp.ErrClosed.Error())
		assertReady(t, stateCh)
		time.Sleep(time.Millisecond * 100)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher starting
[DEBUG] publisher ready
[DEBUG] handle confirmation ready
[DEBUG] handle confirmation started
[DEBUG] channel closed
[DEBUG] handle confirmation stopped
[DEBUG] publisher ready
[DEBUG] handle confirmation ready
[DEBUG] handle confirmation started
[DEBUG] handle confirmation stopped
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})
}

func assertClosed(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-p.NotifyClosed():
	case <-timer.C:
		t.Fatal("publisher close timeout")
	}
}

func assertUnready(t *testing.T, stateCh <-chan publisher.State, errString string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state := <-stateCh:

		require.Nil(t, state.Ready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.EqualError(t, state.Unready.Err, errString)
	case <-timer.C:
		t.Fatal("publisher must be unready")
	}
}

func assertReady(t *testing.T, stateCh <-chan publisher.State) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state := <-stateCh:

		require.Nil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Ready, fmt.Sprintf("%+v", state))
	case <-timer.C:
		t.Fatal("publisher must be ready")
	}
}

func assertNoStateChanged(t *testing.T, stateCh <-chan publisher.State) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()
	select {
	case <-stateCh:
		t.Fatal("state change is not expected")
	case <-timer.C:
	}
}

func newPublisher(opts ...publisher.Option) (connCh chan *publisher.Connection, l *logger.TestLogger, p *publisher.Publisher) {
	connCh = make(chan *publisher.Connection, 1)

	l = logger.NewTest()
	opts = append(opts, publisher.WithLogger(l))

	p, err := publisher.New(connCh, opts...)
	if err != nil {
		panic(err)
	}

	return connCh, l, p
}

func waitResult(resultCh <-chan error, dur time.Duration) error {
	timer := time.NewTimer(dur)
	defer timer.Stop()

	select {
	case res := <-resultCh:
		return res
	case <-timer.C:
		return fmt.Errorf("wait result timeout")
	}
}

func notifyCloseStub() func(_ chan *amqp.Error) chan *amqp.Error {
	return func(ch chan *amqp.Error) chan *amqp.Error {
		return ch
	}
}

func notifyFlowStub() func(_ chan bool) chan bool {
	return func(ch chan bool) chan bool {
		return ch
	}
}

func confirmationChStub(chs ...interface{}) func(chan amqp.Confirmation) chan amqp.Confirmation {
	index := 0
	return func(ch chan amqp.Confirmation) chan amqp.Confirmation {
		switch curr := chs[index].(type) {
		case chan amqp.Confirmation:
			index++
			return curr
		default:
			panic(fmt.Sprintf("unexpected type given: %T", chs[index]))
		}
	}
}

func chCloseChStub(chs ...interface{}) func(chan *amqp.Error) chan *amqp.Error {
	index := 0
	return func(ch chan *amqp.Error) chan *amqp.Error {
		switch curr := chs[index].(type) {
		case chan *amqp.Error:
			index++
			return curr
		default:
			panic(fmt.Sprintf("unexpected type given: %T", chs[index]))
		}
	}
}

func initFuncStub(chs ...interface{}) func(publisher.AMQPConnection) (publisher.AMQPChannel, error) {
	index := 0
	return func(_ publisher.AMQPConnection) (publisher.AMQPChannel, error) {
		switch curr := chs[index].(type) {
		case publisher.AMQPChannel:

			index++
			return curr, nil
		case error:
			index++
			return nil, curr
		default:
			panic(fmt.Sprintf("unexpected type given: %T", chs[index]))
		}
	}
}
