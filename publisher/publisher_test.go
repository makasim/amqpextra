package publisher_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/makasim/amqpextra/publisher/mock_publisher"
)

func TestReconnection(main *testing.T) {
	main.Run("InitFuncRetry", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		retry := 2

		connCh, l, p := newPublisher(
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
		connCh <- newConnStub(amqpConn)
		connCh <- newConnStub(amqpConn)
		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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

		connCh, l, p := newPublisher(
			publisher.WithInitFunc(func(conn publisher.AMQPConnection) (publisher.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
			publisher.WithRestartSleep(time.Millisecond*400),
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		connCh <- newConnStub(amqpConn)

		time.Sleep(time.Millisecond * 200)
		assertUnready(t, p, "the error")

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		newAMQPConn := mock_publisher.NewMockAMQPConnection(ctrl)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch, newCh)))
		defer p.Close()

		conn := newConnStub(amqpConn)
		connCh <- conn
		assertReady(t, p)

		close(conn.StubNotifyClose)
		assertUnready(t, p, amqp.ErrClosed.Error())

		connCh <- newConnStub(newAMQPConn)
		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		conn := newConnStub(amqpConn)
		connCh <- conn
		assertReady(t, p)

		close(conn.StubNotifyClose)
		close(connCh)

		assertClosed(t, p)
		assertUnready(t, p, "permanently closed")

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch, newCh)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)
		assertReady(t, p)

		chCloseCh <- amqp.ErrClosed

		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(any()).
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
		)
		defer p.Close()

		connCh <- newConnStub(amqpConn)
		assertReady(t, p)

		chCloseCh <- amqp.ErrClosed
		assertUnready(t, p, "init func errored")

		connCh <- newConnStub(newAMQPConn)

		assertReady(t, p)

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

func TestUnreadyPublisher(main *testing.T) {
	main.Run("NewPublisherUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p, amqp.ErrClosed.Error())
		p.Close()
		assertClosed(t, p)
	})

	main.Run("ClosedPublisherUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p, amqp.ErrClosed.Error())
		p.Close()
		assertClosed(t, p)
		assertUnready(t, p, "permanently closed")
	})

	main.Run("PublishWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
		defer p.Close()

		resultCh := p.Go(publisher.Message{
			ErrOnUnready: true,
			Publishing:   amqp.Publishing{},
			ResultCh:     nil,
		})

		assertUnready(t, p, amqp.ErrClosed.Error())

		err := waitResult(resultCh, time.Millisecond*100)
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

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
		defer p.Close()

		resultCh := p.Go(publisher.Message{
			ErrOnUnready: true,
			Publishing:   amqp.Publishing{},
			ResultCh:     make(chan error, 1),
		})

		assertUnready(t, p, amqp.ErrClosed.Error())

		err := waitResult(resultCh, time.Millisecond*100)
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

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
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

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		go func() {
			time.Sleep(time.Millisecond * 400)
			amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

			connCh <- newConnStub(amqpConn)
		}()

		assertUnready(t, p, amqp.ErrClosed.Error())

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(fmt.Errorf("publish errored")).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(fmt.Errorf("publish errored")).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			MaxTimes(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(nil).
			MaxTimes(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			AnyTimes()

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(nil).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		go func() {
			<-time.NewTimer(time.Second).C

			connCh <- newConnStub(amqpConn)
		}()

		assertUnready(t, p, amqp.ErrClosed.Error())

		before := time.Now().UnixNano()
		resultCh := p.Go(publisher.Message{
			Publishing: amqp.Publishing{},
			ResultCh:   make(chan error, 1),
		})

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

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		resultCh := p.Go(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
		})

		err := waitResult(resultCh, time.Millisecond*1300)
		require.EqualError(t, err, "publisher stopped")

		expected := `[DEBUG] publisher starting
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		resultCh := p.Go(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
			ResultCh:   make(chan error, 1),
		})

		assertUnready(t, p, "permanently closed")

		err := waitResult(resultCh, time.Millisecond*100)
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

		connCh := make(chan publisher.ConnectionReady)

		p := publisher.New(connCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p, amqp.ErrClosed.Error())

		p.Close()
		p.Close()
		assertClosed(t, p)
	})

	main.Run("CloseUnreadyByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.ConnectionReady)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		p := publisher.New(connCh, publisher.WithContext(ctx), publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p, amqp.ErrClosed.Error())

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		connCh, l, p := newPublisher(
			publisher.WithContext(ctx),
			publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(fmt.Errorf("channel close errored")).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(amqp.ErrClosed).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch, newCh)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)
		conn := newConnStub(amqpConn)
		connCh <- conn

		assertReady(t, p)

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < 10; i++ {
					<-p.Go(publisher.Message{})
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		close(conn.StubNotifyClose)

		connCh <- newConnStub(newAMQPConn)

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

		var chCloseCh chan *amqp.Error

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)

		newCh := mock_publisher.NewMockAMQPChannel(ctrl)
		newCh.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		newCh.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(notifyFlowStub()).
			Times(1)
		newCh.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
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

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch, newCh)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(nil).
			Times(3)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

		resultCh := make(chan error, 1)

		p.Go(publisher.Message{ResultCh: resultCh})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

		go p.Go(publisher.Message{ResultCh: resultCh})
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "wait result timeout")

		chFlowCh <- true
		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(nil).
			Times(2)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

		resultCh := make(chan error, 1)

		p.Go(publisher.Message{
			ResultCh:     resultCh,
			ErrOnUnready: true,
		})
		require.NoError(t, waitResult(resultCh, time.Millisecond*50))

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

		go p.Go(publisher.Message{
			ResultCh:     resultCh,
			ErrOnUnready: true,
		})
		require.EqualError(t, waitResult(resultCh, time.Millisecond*100), "publisher not ready")

		chFlowCh <- true
		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(any(), any(), any(), any(), any()).
			Return(nil).
			Times(3)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)

		assertReady(t, p)

		p.Go(publisher.Message{})

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

		waitResult := make(chan struct{})
		go func() {
			defer close(waitResult)
			p.Go(publisher.Message{})
		}()

		chFlowCh <- true
		assertReady(t, p)

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

		var chFlowCh chan bool

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(1)
		ch.
			EXPECT().
			NotifyFlow(any()).
			DoAndReturn(func(ch chan bool) chan bool {
				chFlowCh = ch
				return ch
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			Return(nil).
			Times(1)

		connCh, l, p := newPublisher(publisher.WithInitFunc(initFuncStub(ch)))
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)
		assertReady(t, p)

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(2)
		ch.
			EXPECT().
			NotifyFlow(any()).
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
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		connCh <- newConnStub(amqpConn)
		assertReady(t, p)

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

		chCloseCh <- amqp.ErrClosed
		assertReady(t, p)

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

		ch := mock_publisher.NewMockAMQPChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(any()).
			DoAndReturn(notifyCloseStub()).
			Times(2)
		ch.
			EXPECT().
			NotifyFlow(any()).
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
		)
		defer p.Close()

		amqpConn := mock_publisher.NewMockAMQPConnection(ctrl)

		conn := newConnStub(amqpConn)
		connCh <- conn
		assertReady(t, p)

		chFlowCh <- false
		assertUnready(t, p, "publisher flow paused")

		close(conn.StubNotifyClose)
		connCh <- newConnStub(amqpConn)
		assertReady(t, p)

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

func assertReady(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-p.NotifyReady():
	case <-timer.C:
		t.Fatal("publisher must be ready")
	}
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

func assertUnready(t *testing.T, p *publisher.Publisher, errString string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case actualErr, ok := <-p.NotifyUnready():
		if !ok {
			require.Equal(t, "permanently closed", errString)
			return
		}

		require.EqualError(t, actualErr, errString)
	case <-timer.C:
		t.Fatal("publisher must be unready")
	}
}

func any() gomock.Matcher {
	return gomock.Any()
}

func newPublisher(opts ...publisher.Option) (connReadyCh chan publisher.ConnectionReady, l *logger.TestLogger, p *publisher.Publisher) {
	connReadyCh = make(chan publisher.ConnectionReady, 1)

	l = logger.NewTest()
	opts = append(opts, publisher.WithLogger(l))

	p = publisher.New(connReadyCh, opts...)

	return connReadyCh, l, p
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

func newConnStub(conn publisher.AMQPConnection) *connReadyStub {
	return &connReadyStub{
		StubConn:        conn,
		StubNotifyClose: make(chan struct{}, 1),
	}
}

type connReadyStub struct {
	StubConn        publisher.AMQPConnection
	StubNotifyClose chan struct{}
}

func (s *connReadyStub) Conn() publisher.AMQPConnection {
	return s.StubConn
}

func (s *connReadyStub) NotifyClose() chan struct{} {
	return s.StubNotifyClose
}
