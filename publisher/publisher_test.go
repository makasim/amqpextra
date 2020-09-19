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

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		retry := 2

		connCh, _, l, p := newPublisher(
			publisher.WithInitFunc(func(conn publisher.Connection) (publisher.Channel, error) {
				if retry > 0 {
					retry--
					return nil, fmt.Errorf("init func errored: %d", retry)
				}

				return ch, nil
			}),
			publisher.WithRestartSleep(time.Millisecond*10),
		)
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		connCh <- conn
		connCh <- conn
		connCh <- conn

		assertReady(t, p)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[ERROR] init func: init func errored: 1
[DEBUG] publisher started
[ERROR] init func: init func errored: 0
[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConnectionClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)

		connCh, closeCh, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh <- conn
		assertReady(t, p)

		closeCh <- amqp.ErrClosed

		conn = mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh <- conn
		assertReady(t, p)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ConnectionPermanentlyClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)

		connCh, closeCh, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh <- conn
		assertReady(t, p)

		closeCh <- amqp.ErrClosed
		close(connCh)

		assertUnready(t, p)
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("ChannelClosed", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		chCloseCh := make(chan *amqp.Error)

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

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
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh <- conn
		assertReady(t, p)

		chCloseCh <- amqp.ErrClosed

		newCh := mock_publisher.NewMockChannel(ctrl)

		newConn := mock_publisher.NewMockConnection(ctrl)
		newConn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return newCh, nil
		}).Times(1)

		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		newCh.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh <- newConn
		assertReady(t, p)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] channel closed
[DEBUG] publisher started
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

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)
		p.Close()
		assertClosed(t, p)
	})

	main.Run("ClosedPublisherUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)
		p.Close()
		assertClosed(t, p)
		assertUnready(t, p)
	})

	main.Run("PublishWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		p.Publish(publisher.Message{
			WaitReady:  false,
			Publishing: amqp.Publishing{},
			ResultCh:   nil,
		})

		assertUnready(t, p)
		p.Close()
		assertClosed(t, p)

		expected := `[ERROR] publisher not ready
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		resultCh := make(chan error, 1)

		p.Publish(publisher.Message{
			WaitReady:  false,
			Publishing: amqp.Publishing{},
			ResultCh:   resultCh,
		})

		assertUnready(t, p)

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publisher not ready")

		p.Close()
		assertClosed(t, p)

		expected := ``
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithUnbufferedResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assert.PanicsWithValue(t, "amqpextra: resultCh channel is unbuffered", func() {
			p.Publish(publisher.Message{
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

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		go func() {
			time.Sleep(time.Millisecond * 400)
			conn := mock_publisher.NewMockConnection(ctrl)
			conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
				return ch, nil
			}).Times(1)

			connCh <- conn
		}()

		assertUnready(t, p)

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond * 200)
			cancelFunc()
		}()

		p.Publish(publisher.Message{
			Context:    msgCtx,
			WaitReady:  true,
			Publishing: amqp.Publishing{},
		})

		time.Sleep(time.Millisecond * 100)
		p.Close()
		assertClosed(t, p)

		expected := `[ERROR] message: context canceled
`
		require.Equal(t, expected, l.Logs())
	})
}

func TestReadyPublisher(main *testing.T) {
	main.Run("PublishExpectedMessage", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
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
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		resultCh := make(chan error, 1)
		p.Publish(publisher.Message{
			Exchange:  "theExchange",
			Key:       "theKey",
			Mandatory: true,
			Immediate: true,
			WaitReady: true,
			ResultCh:  resultCh,
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

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishErroredWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return fmt.Errorf("publish errored")
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		p.Publish(publisher.Message{
			WaitReady:  true,
			Publishing: amqp.Publishing{},
		})

		time.Sleep(time.Millisecond * 100)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[ERROR] publish errored
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishErroredWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return fmt.Errorf("publish errored")
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		resultCh := make(chan error, 1)
		p.Publish(publisher.Message{
			WaitReady:  true,
			ResultCh:   resultCh,
			Publishing: amqp.Publishing{},
		})

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, "publish errored")

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishMessageContextClosedBeforePublish", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		p.Publish(publisher.Message{
			Context:    msgCtx,
			WaitReady:  true,
			Publishing: amqp.Publishing{},
		})

		time.Sleep(time.Millisecond * 100)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[ERROR] message: context canceled
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWaitForReady", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		go func() {
			<-time.NewTimer(time.Second).C

			connCh <- conn
		}()

		assertUnready(t, p)

		resultCh := make(chan error, 1)
		before := time.Now().UnixNano()
		p.Publish(publisher.Message{
			WaitReady:  true,
			Publishing: amqp.Publishing{},
			ResultCh:   resultCh,
		})

		err := waitResult(resultCh, time.Millisecond*1300)
		require.NoError(t, err)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
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

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		p.Publish(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
			ResultCh:   nil,
		})

		expected := `[ERROR] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		resultCh := make(chan error, 1)

		p.Publish(publisher.Message{
			Context:    context.Background(),
			Publishing: amqp.Publishing{},
			ResultCh:   resultCh,
		})

		assertUnready(t, p)

		err := waitResult(resultCh, time.Millisecond*100)
		require.EqualError(t, err, `publisher stopped`)

		expected := ``
		require.Equal(t, expected, l.Logs())
	})
}

func TestClose(main *testing.T) {
	main.Run("CloseTwice", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New2(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)

		p.Close()
		p.Close()
		assertClosed(t, p)
	})

	main.Run("CloseUnreadyByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.NewTest()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		p := publisher.New2(connCh, closeCh, publisher.WithContext(ctx), publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)

		cancelFunc()
		assertClosed(t, p)

		expected := ``
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseReadyByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return nil
			}).
			Times(1)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		connCh, _, l, p := newPublisher(publisher.WithContext(ctx))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		cancelFunc()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return fmt.Errorf("channel close errored")
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[WARN] publisher: channel close: channel close errored
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseIgnoreClosedError", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				return nil
			}).
			Times(1)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
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

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh, _, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

		connCh <- conn

		assertReady(t, p)

		for i := 0; i < 10; i++ {
			go func() {
				resultCh := make(chan error, 1)
				for i := 0; i < 10; i++ {
					p.Publish(publisher.Message{
						ResultCh: resultCh,
					})
					<-resultCh
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseConnectionWhilePublishing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		ch := mock_publisher.NewMockChannel(ctrl)
		ch.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		ch.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh, closeCh, l, p := newPublisher()
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)
		connCh <- conn

		assertReady(t, p)

		for i := 0; i < 10; i++ {
			go func() {
				resultCh := make(chan error, 1)
				for i := 0; i < 10; i++ {
					p.Publish(publisher.Message{
						ResultCh: resultCh,
					})
					<-resultCh
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		closeCh <- amqp.ErrClosed

		newCh := mock_publisher.NewMockChannel(ctrl)

		newConn := mock_publisher.NewMockConnection(ctrl)
		newConn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return newCh, nil
		}).Times(1)

		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				return receiver
			}).
			Times(1)
		newCh.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(60).
			MaxTimes(80)
		newCh.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh <- newConn

		time.Sleep(time.Millisecond * 900)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("CloseChannelWhilePublishing", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)

		var chCloseCh chan *amqp.Error

		connCh, _, l, p := newPublisher()
		defer p.Close()

		ch := mock_publisher.NewMockChannel(ctrl)

		conn := mock_publisher.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return ch, nil
		}).Times(1)

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
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(20).
			MaxTimes(40)
		ch.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh <- conn

		assertReady(t, p)

		for i := 0; i < 10; i++ {
			go func() {
				resultCh := make(chan error, 1)
				for i := 0; i < 10; i++ {
					p.Publish(publisher.Message{
						ResultCh: resultCh,
					})
					<-resultCh
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		chCloseCh <- amqp.ErrClosed

		newCh := mock_publisher.NewMockChannel(ctrl)

		newConn := mock_publisher.NewMockConnection(ctrl)
		newConn.EXPECT().Channel().DoAndReturn(func() (publisher.Channel, error) {
			return newCh, nil
		}).Times(1)

		newCh.
			EXPECT().
			NotifyClose(gomock.Any()).
			DoAndReturn(func(receiver chan *amqp.Error) chan *amqp.Error {
				chCloseCh = receiver
				return receiver
			}).
			Times(1)
		newCh.
			EXPECT().
			Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
				time.Sleep(time.Millisecond * 10)
				return nil
			}).
			MinTimes(60).
			MaxTimes(80)
		newCh.
			EXPECT().
			Close().
			DoAndReturn(func() error {
				return amqp.ErrClosed
			}).
			Times(1)

		connCh <- newConn

		time.Sleep(time.Millisecond * 900)
		p.Close()
		assertClosed(t, p)

		expected := `[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] channel closed
[DEBUG] publisher started
[DEBUG] publisher ready
[DEBUG] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

}

func assertReady(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-p.Ready():
	case <-timer.C:
		t.Fatal("publisher must be ready")
	}
}

func assertClosed(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-p.Closed():
	case <-timer.C:
		t.Fatal("publisher close timeout")
	}
}

func assertUnready(t *testing.T, p *publisher.Publisher) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-p.Unready():
	case <-timer.C:
		t.Fatal("publisher must be unready")
	}
}

func newPublisher(opts ...publisher.Option) (connCh chan publisher.Connection, closeCh chan *amqp.Error, l *logger.TestLogger, p *publisher.Publisher) {
	connCh = make(chan publisher.Connection, 1)
	closeCh = make(chan *amqp.Error, 1)

	l = logger.NewTest()
	opts = append(opts, publisher.WithLogger(l))

	p = publisher.New2(connCh, closeCh, opts...)

	return connCh, closeCh, l, p
}

func waitResult(resultCh chan error, dur time.Duration) error {
	timer := time.NewTimer(dur)
	defer timer.Stop()

	select {
	case res := <-resultCh:
		return res
	case <-timer.C:
		return fmt.Errorf("wait result timeout")
	}
}
