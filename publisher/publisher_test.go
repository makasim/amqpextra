package publisher_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/makasim/amqpextra/test/e2e/helper/logger"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	"github.com/makasim/amqpextra/publisher/mock_publisher"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestServe(main *testing.T) {
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

		connCh, closeCh, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
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

		connCh, closeCh, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		chCloseCh := make(chan *amqp.Error)

		conn := mock_publisher.NewMockConnection(ctrl)
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

		conn = mock_publisher.NewMockConnection(ctrl)
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
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)
		p.Close()
		assertClosed(t, p)
	})

	main.Run("ClosedPublisherUnready", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)
		p.Close()
		assertClosed(t, p)
		assertUnready(t, p)
	})

	main.Run("PublishWithNoResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		p.Publish(amqpextra.Publishing{
			WaitReady: false,
			Message:   amqp.Publishing{},
			ResultCh:  nil,
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
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		resultCh := make(chan error, 1)

		p.Publish(amqpextra.Publishing{
			WaitReady: false,
			Message:   amqp.Publishing{},
			ResultCh:  resultCh,
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
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assert.PanicsWithValue(t, "amqpextra: resultCh channel is unbuffered", func() {
			p.Publish(amqpextra.Publishing{
				Context:  context.Background(),
				Message:  amqp.Publishing{},
				ResultCh: make(chan error),
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		go func() {
			time.Sleep(time.Millisecond * 400)
			conn := mock_publisher.NewMockConnection(ctrl)
			connCh <- conn
		}()

		assertUnready(t, p)

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond * 200)
			cancelFunc()
		}()

		p.Publish(amqpextra.Publishing{
			Context:   msgCtx,
			WaitReady: true,
			Message:   amqp.Publishing{},
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		connCh <- conn

		assertReady(t, p)

		resultCh := make(chan error, 1)
		p.Publish(amqpextra.Publishing{
			Exchange:  "theExchange",
			Key:       "theKey",
			Mandatory: true,
			Immediate: true,
			WaitReady: true,
			ResultCh:  resultCh,
			Message: amqp.Publishing{
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		connCh <- conn

		assertReady(t, p)

		p.Publish(amqpextra.Publishing{
			WaitReady: true,
			Message:   amqp.Publishing{},
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		connCh <- conn

		assertReady(t, p)

		resultCh := make(chan error, 1)
		p.Publish(amqpextra.Publishing{
			WaitReady: true,
			ResultCh:  resultCh,
			Message:   amqp.Publishing{},
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		connCh <- conn

		assertReady(t, p)

		msgCtx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		p.Publish(amqpextra.Publishing{
			Context:   msgCtx,
			WaitReady: true,
			Message:   amqp.Publishing{},
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
		go func() {
			<-time.NewTimer(time.Second).C

			connCh <- conn
		}()

		assertUnready(t, p)

		resultCh := make(chan error, 1)
		before := time.Now().UnixNano()
		p.Publish(amqpextra.Publishing{
			WaitReady: true,
			Message:   amqp.Publishing{},
			ResultCh:  resultCh,
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
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		p.Publish(amqpextra.Publishing{
			Context:  context.Background(),
			Message:  amqp.Publishing{},
			ResultCh: nil,
		})

		expected := `[ERROR] publisher stopped
`
		require.Equal(t, expected, l.Logs())
	})

	main.Run("PublishWithResultChannel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))

		p.Close()
		assertClosed(t, p)

		resultCh := make(chan error, 1)

		p.Publish(amqpextra.Publishing{
			Context:  context.Background(),
			Message:  amqp.Publishing{},
			ResultCh: resultCh,
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
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		p := publisher.New(connCh, closeCh, publisher.WithLogger(l))
		defer p.Close()

		assertUnready(t, p)

		p.Close()
		p.Close()
		assertClosed(t, p)
	})

	main.Run("CloseUnreadyByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		l := logger.New()

		connCh := make(chan publisher.Connection)
		closeCh := make(chan *amqp.Error)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		p := publisher.New(connCh, closeCh, publisher.WithContext(ctx), publisher.WithLogger(l))
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

		connCh, _, l, p := newPublisher(publisher.WithContext(ctx), withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
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

		connCh, _, l, p := newPublisher(withInitFuncMock(ch))
		defer p.Close()

		conn := mock_publisher.NewMockConnection(ctrl)
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

// func TestPublishConsumeWaitReady(t *testing.T) {
// 	defer goleak.VerifyNone(t)
// 	log.Print(7)
// 	l := logger.New()
//
// 	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
// 	require.NoError(t, err)
// 	defer conn.Close()
//
// 	ch, err := conn.Channel()
// 	require.NoError(t, err)
//
// 	q, err := ch.QueueDeclare("test-publish-with-wait-ready", true, false, false, false, amqp.Table{})
// 	require.NoError(t, err)
//
// 	connCh := make(chan *amqp.Connection, 1)
//
// 	go func() {
// 		<-time.NewTimer(100 * time.Millisecond).C
//
// 		connCh <- conn
// 	}()
//
// 	connCloseCh := make(chan *amqp.Error)
//
// 	p := amqpextra.NewPublisher(connCh, connCloseCh)
// 	defer p.Close()
// 	p.SetLogger(l)
//
// 	resultCh := make(chan error, 1)
//
// 	ctx, cancelFunc := context.WithTimeout(context.Background(), 200*time.Millisecond)
// 	defer cancelFunc()
//
// 	p.Publish(amqpextra.Publishing{
// 		Key:       q.Name,
// 		Context:   ctx,
// 		WaitReady: true,
// 		Message: amqp.Publishing{
// 			Body: []byte(`testPayload`),
// 		},
// 		ResultCh: resultCh,
// 	})
//
// 	err = waitResult(resultCh, time.Millisecond*100)
// 	require.NoError(t, err)
//
// 	msgCh, err := ch.Consume(q.Name, "", true, false, false, false, amqp.Table{})
// 	require.NoError(t, err)
//
// 	timer := time.NewTimer(time.Millisecond * 100)
// 	defer timer.Stop()
// 	select {
// 	case msg, ok := <-msgCh:
// 		require.True(t, ok)
//
// 		require.Equal(t, `testPayload`, string(msg.Body))
// 	case <-timer.C:
// 		t.Fatal("wait delivery timeout")
// 	}
// }
//
//
// func TestPublishCloseChannelPublish(t *testing.T) {
// 	defer goleak.VerifyNone(t)
// 	log.Print(8)
// 	l := logger.New()
//
// 	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
// 	require.NoError(t, err)
// 	defer conn.Close()
//
// 	ch, err := conn.Channel()
// 	require.NoError(t, err)
//
// 	q, err := ch.QueueDeclare("test-publish-close-channel", true, false, false, false, amqp.Table{})
// 	require.NoError(t, err)
//
// 	connCh := make(chan *amqp.Connection, 2)
// 	connCh <- conn
// 	connCh <- conn
//
// 	connCloseCh := make(chan *amqp.Error)
//
// 	p := amqpextra.NewPublisher(connCh, connCloseCh)
// 	defer p.Close()
// 	p.SetLogger(l)
//
// 	timer := time.NewTimer(time.Millisecond * 100)
// 	defer timer.Stop()
// 	select {
// 	case publisherChannel := <-p.Channel():
// 		require.NoError(t, publisherChannel.Close())
// 		log.Print(123, l.Logs())
// 		resultCh := make(chan error, 1)
// 		p.Publish(amqpextra.Publishing{
// 			Key:       q.Name,
// 			WaitReady: true,
// 			Message: amqp.Publishing{
// 				Body: []byte(`testPayload`),
// 			},
// 			ResultCh: resultCh,
// 		})
// 		log.Print(124, l.Logs())
// 		err = waitResult(resultCh, time.Millisecond*100)
// 		assert.NoError(t, err)
// 		log.Print(125)
// 		p.Publish(amqpextra.Publishing{
// 			Key:       q.Name,
// 			WaitReady: true,
// 			Message: amqp.Publishing{
// 				Body: []byte(`testPayload`),
// 			},
// 			ResultCh: resultCh,
// 		})
// 		log.Print(126)
// 		err = waitResult(resultCh, time.Millisecond*100)
// 		assert.NoError(t, err)
// 		log.Print(127)
// 		expected := ``
// 		require.Equal(t, expected, l.Logs())
// 	case <-timer.C:
// 		t.Fatal("get publisher channel timeout")
// 	}
// }
//
// func TestPublishConsumeContextDeadline(t *testing.T) {
// 	defer goleak.VerifyNone(t)
// 	log.Print(9)
// 	l := logger.New()
//
// 	connCh := make(chan *amqp.Connection, 1)
//
// 	connCloseCh := make(chan *amqp.Error)
//
// 	p := amqpextra.NewPublisher(connCh, connCloseCh)
// 	defer p.Close()
// 	p.SetLogger(l)
//
// 	resultCh := make(chan error, 1)
//
// 	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
// 	defer cancelFunc()
//
// 	p.Publish(amqpextra.Publishing{
// 		Key:       "a_queue",
// 		Context:   ctx,
// 		WaitReady: true,
// 		Message: amqp.Publishing{
// 			Body: []byte(`testPayload`),
// 		},
// 		ResultCh: resultCh,
// 	})
//
// 	err := waitResult(resultCh, time.Millisecond*100)
// 	require.Equal(t, err, context.DeadlineExceeded)
// }
//
// func TestPublishConsumeContextCanceled(t *testing.T) {
// 	defer goleak.VerifyNone(t)
// 	log.Print(10)
// 	l := logger.New()
//
// 	connCh := make(chan *amqp.Connection, 1)
//
// 	connCloseCh := make(chan *amqp.Error)
//
// 	p := amqpextra.NewPublisher(connCh, connCloseCh)
// 	defer p.Close()
// 	p.SetLogger(l)
//
// 	resultCh := make(chan error, 1)
//
// 	ctx, cancelFunc := context.WithCancel(context.Background())
// 	cancelFunc()
//
// 	p.Publish(amqpextra.Publishing{
// 		Key:       "a_queue",
// 		Context:   ctx,
// 		WaitReady: true,
// 		Message: amqp.Publishing{
// 			Body: []byte(`testPayload`),
// 		},
// 		ResultCh: resultCh,
// 	})
//
// 	err := waitResult(resultCh, time.Millisecond*100)
// 	require.Equal(t, err, context.Canceled)
// }
//
// func TestConcurrentlyPublishConsumeWhileConnectionLost(t *testing.T) {
// 	defer goleak.VerifyNone(t)
// 	log.Print(11)
// 	l := logger.New()
//
// 	consumerConn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
// 	assert.NoError(t, err)
// 	defer consumerConn.Close()
//
// 	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())
//
// 	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
// 		Properties: amqp.Table{
// 			"connection_name": connName,
// 		},
// 	})
// 	defer conn.Close()
// 	conn.SetLogger(l)
//
// 	var wg sync.WaitGroup
//
// 	wg.Add(1)
// 	go func(connName string, wg *sync.WaitGroup) {
// 		defer wg.Done()
//
// 		<-time.NewTimer(time.Second * 5).C
// 		if !assert.True(t, rabbitmq.CloseConn(connName)) {
// 			return
// 		}
// 	}(connName, &wg)
//
// 	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())
// 	var countPublished uint32
// 	for i := 0; i < 5; i++ {
// 		wg.Add(1)
// 		go func(extraconn *amqpextra.Connection, queue string, wg *sync.WaitGroup) {
// 			defer wg.Done()
// 			connCh, connCloseCh := extraconn.ConnCh()
//
// 			ticker := time.NewTicker(time.Millisecond * 100)
// 			defer ticker.Stop()
//
// 			timer := time.NewTimer(time.Second * 10)
// 			defer timer.Stop()
//
// 			p := amqpextra.NewPublisher(connCh, connCloseCh)
// 			p.SetLogger(l)
//
// 			resultCh := make(chan error, 1)
//
// 			for {
// 				select {
// 				case <-ticker.C:
// 					p.Publish(amqpextra.Publishing{
// 						Key:       queue,
// 						WaitReady: true,
// 						ResultCh:  resultCh,
// 					})
//
// 					if err := waitResult(resultCh, time.Millisecond*100); err == nil {
// 						atomic.AddUint32(&countPublished, 1)
// 					} else {
// 						t.Errorf("publish errored: %s", err)
// 					}
// 				case <-timer.C:
// 					p.Close()
//
// 					return
// 				}
// 			}
// 		}(conn, queue, &wg)
// 	}
//
// 	var countConsumed uint32
// 	for i := 0; i < 5; i++ {
// 		wg.Add(1)
//
// 		timer := time.NewTimer(time.Second * 11)
//
// 		go rabbitmq.ConsumeReconnect(consumerConn, timer, queue, &countConsumed, &wg)
// 	}
//
// 	wg.Wait()
//
// 	expected := `[DEBUG] connection established
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] connection established
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher started
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// [DEBUG] publisher stopped
// `
// 	require.Equal(t, expected, l.Logs())
//
// 	require.GreaterOrEqual(t, countPublished, uint32(200))
// 	require.LessOrEqual(t, countPublished, uint32(520))
//
// 	require.GreaterOrEqual(t, countConsumed, uint32(200))
// 	require.LessOrEqual(t, countConsumed, uint32(520))
// }

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

func newPublisher(opts ...publisher.Option) (connCh chan publisher.Connection, closeCh chan *amqp.Error, l *logger.Logger, p *publisher.Publisher) {
	connCh = make(chan publisher.Connection, 1)
	closeCh = make(chan *amqp.Error, 1)

	l = logger.New()
	opts = append(opts, publisher.WithLogger(l))

	p = publisher.New(connCh, closeCh, opts...)

	return connCh, closeCh, l, p
}

func withInitFuncMock(ch publisher.Channel) publisher.Option {
	return publisher.WithInitFunc(func(conn publisher.Connection) (publisher.Channel, error) {
		return ch, nil
	})
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
