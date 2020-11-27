package consumer_test

import (
	"testing"

	"time"

	"context"

	"fmt"

	"sync"

	"github.com/golang/mock/gomock"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/consumer/mock_consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNotify(main *testing.T) {
	main.Run("PanicIfStateChUnbuffered", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State)

		require.PanicsWithValue(t, "state chan is unbuffered", func() {
			c, _ := consumer.New(
				connCh,
				consumer.WithQueue("theQueue"),
				consumer.WithHandler(h),
			)
			defer c.Close()
			c.Notify(stateCh)
		})
	})

	main.Run("UnreadyWhileInit", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				time.Sleep(time.Millisecond * 20)
				return nil, fmt.Errorf("the error")
			}),
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithLogger(l),
		)
		require.NoError(t, err)

		defer c.Close()

		newStateCh := c.Notify(stateCh)
		assertUnready(t, stateCh, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, newStateCh, "the error")
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ReadyIfConnected", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume(any(), any(), any(), any(), any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().Close().AnyTimes()

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithLogger(l),
		)
		require.NoError(t, err)

		defer c.Close()

		newStateCh := c.Notify(stateCh)
		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, newStateCh, amqp.ErrClosed.Error())
		assertReady(t, newStateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

}

func TestUnready(main *testing.T) {
	main.Run("CloseByMethod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
		)
		require.NoError(t, err)

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseByContext", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithContext(ctx),
			consumer.WithNotify(stateCh),
		)
		require.NoError(t, err)

		cancelFunc()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseByConnChannelClose", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
		)
		require.NoError(t, err)

		close(connCh)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseOnInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithNotify(stateCh),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)
		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, stateCh, "the error")
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseConnChAfterInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")
		close(connCh)
		time.Sleep(time.Millisecond * 50)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("UnreadyAfterInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseConnChAfterInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithNotify(stateCh),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, stateCh, "the error")
		time.Sleep(time.Millisecond * 50)
		close(connCh)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseWhileInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, stateCh, "the error")
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseConnChAfterInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("aQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*50),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertUnready(t, stateCh, "the error")
		time.Sleep(time.Millisecond * 50)
		close(connCh)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseWhileConsumeRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume("theQueue", any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*50),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] ch.Consume: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseConnChAfterConsumeRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume("theQueue", any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*50),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 55)
		close(connCh)
		assertUnready(t, stateCh, "the error")
		time.Sleep(time.Millisecond * 50)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] ch.Consume: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})
}

func TestConsume(main *testing.T) {
	main.Run("NoMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)
		stateCh := make(chan consumer.State, 2)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithLogger(l),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ConsumeDefaultArguments", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		stateCh := make(chan consumer.State, 2)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume("theQueue", "", false, false, false, false, amqp.Table(nil)).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)
		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ConsumeCustomArguments", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		stateCh := make(chan consumer.State, 2)
		msgCh := make(chan amqp.Delivery)

		table := amqp.Table{"foo": "fooVal"}

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume("theQueue", "theConsumer", true, true, true, true, table).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithConsumeArgs("theConsumer", true, true, true, true, table),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ConnClosedWhileWaitingMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)
		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		amqpConn := mock_consumer.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := consumer.NewConnection(amqpConn, closeCh)
		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithLogger(l),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- conn
		assertReady(t, stateCh, "theQueue")

		close(connCh)
		close(closeCh)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ChannelClosedWhileWaitingMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		newChCloseCh := make(chan *amqp.Error)
		newCancelCh := make(chan string)
		newNsgCh := make(chan amqp.Delivery)

		newCh := mock_consumer.NewMockAMQPChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(newNsgCh, nil).Times(1)
		newCh.EXPECT().Qos(any(), any(), any()).
			Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*1),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)
		defer c.Close()

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		time.Sleep(time.Millisecond * 50)
		chCloseCh <- amqp.ErrClosed
		assertUnready(t, stateCh, "channel closed")
		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] channel closed
[DEBUG] worker stopped
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ChannelCloseErroredWhileWaitingMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)
		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(fmt.Errorf("the error")).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)
		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[WARN] channel close: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ChannelAlreadyClosedErrorWhileWaitingMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(amqp.ErrClosed).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ConsumptionCancelledWhileWaitingMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		newChCloseCh := make(chan *amqp.Error)
		newCancelCh := make(chan string)
		newMsgCh := make(chan amqp.Delivery)
		stateCh := make(chan consumer.State, 2)

		newConn := mock_consumer.NewMockAMQPConnection(ctrl)

		newCh := mock_consumer.NewMockAMQPChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(newMsgCh, nil).Times(1)
		newCh.EXPECT().Qos(any(), any(), any()).Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Times(1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		time.Sleep(time.Millisecond * 50)
		cancelCh <- "aTag"
		assertUnready(t, stateCh, "consumption canceled")

		connCh <- consumer.NewConnection(newConn, nil)

		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] consumption canceled
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("GotSomeMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		msgCh <- amqp.Delivery{}
		msgCh <- amqp.Delivery{}
		msgCh <- amqp.Delivery{}
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[TEST] got message
[TEST] got message
[TEST] got message
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})
}

func TestConcurrency(main *testing.T) {
	main.Run("CloseConnectionWhileConsuming", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		countConsumed := 0
		l := logger.NewTest()
		h := handlerCounter(&countConsumed)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)
		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		amqpConn := mock_consumer.NewMockAMQPConnection(ctrl)

		newChCloseCh := make(chan *amqp.Error)
		newCancelCh := make(chan string)
		newCh := mock_consumer.NewMockAMQPChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		newCh.EXPECT().Qos(any(), any(), any()).
			Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		newConn := mock_consumer.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := consumer.NewConnection(amqpConn, closeCh)
		connCh := make(chan *consumer.Connection, 2)

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					msgCh <- amqp.Delivery{}
				}
			}()
		}

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)
		connCh <- conn

		connCh <- consumer.NewConnection(newConn, nil)

		assertReady(t, stateCh, "theQueue")
		time.Sleep(time.Millisecond * 300)
		close(closeCh)
		wg.Wait()
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, 100, countConsumed)
		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseWhileConsuming", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		countConsumed := 0
		l := logger.NewTest()
		h := handlerCounter(&countConsumed)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(amqp.ErrClosed).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 2)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					select {
					case msgCh <- amqp.Delivery{}:
					case <-c.NotifyClosed():
						return
					}
				}
			}()
		}

		time.Sleep(time.Millisecond * 300)
		c.Close()
		wg.Wait()
		assertClosed(t, c)

		assert.Greater(t, countConsumed, 20)
		assert.Less(t, countConsumed, 40)
		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("CloseChannelWhileConsuming", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		countConsumed := 0
		l := logger.NewTest()
		h := handlerCounter(&countConsumed)

		chCloseCh := make(chan *amqp.Error)
		cancelCh := make(chan string)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		newChCloseCh := make(chan *amqp.Error)
		newCancelCh := make(chan string)
		newCh := mock_consumer.NewMockAMQPChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		newCh.EXPECT().Qos(any(), any(), any()).
			Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 2)
		stateCh := make(chan consumer.State, 2)

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					msgCh <- amqp.Delivery{}
				}
			}()
		}

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		time.Sleep(time.Millisecond * 300)
		chCloseCh <- amqp.ErrClosed
		wg.Wait()
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, 100, countConsumed)
		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] channel closed
[DEBUG] worker stopped
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})
}

func TestOptions(main *testing.T) {
	main.Run("ReadyWithQueue", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		stateCh := make(chan consumer.State, 2)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().
			Consume("theQueue", any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).
			AnyTimes()
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ReadyWithExchange", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		stateCh := make(chan consumer.State, 2)
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().
			Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).
			AnyTimes()
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			QueueDeclare(any(), any(), any(), any(), any(), any()).
			Return(amqp.Queue{Name: "theTmpQueue"}, nil)
		ch.EXPECT().
			QueueBind("theTmpQueue", "theRoutingKey", "theExchange", any(), any()).
			Return(nil)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithExchange("theExchange", "theRoutingKey"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theTmpQueue")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker starting
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("WaitIfQueueDeclareErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().
			QueueDeclare(any(), any(), any(), any(), any(), any()).
			Return(amqp.Queue{}, fmt.Errorf("the error"))
		ch.EXPECT().
			QueueBind(any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithExchange("aExchange", "aRoutingKey"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("WaitIfQueueBindErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().
			QueueDeclare(any(), any(), any(), any(), any(), any()).
			Return(amqp.Queue{}, nil)
		ch.EXPECT().
			QueueBind(any(), any(), any(), any(), any()).
			Return(fmt.Errorf("the error"))
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithExchange("aExchange", "aRoutingKey"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("WaitIfQueueBindErrored", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		stateCh := make(chan consumer.State, 2)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().
			QueueDeclare(any(), any(), any(), any(), any(), any()).
			Return(amqp.Queue{}, nil)
		ch.EXPECT().
			QueueBind(any(), any(), any(), any(), any()).
			Return(fmt.Errorf("the error"))
		ch.EXPECT().Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithExchange("aExchange", "aRoutingKey"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithNotify(stateCh),
			consumer.WithRetryPeriod(time.Millisecond*100),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 50)
		assertUnready(t, stateCh, "the error")

		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("ErroredIfHandlerIsNil", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		_, err := consumer.New(
			connCh,
			consumer.WithHandler(nil),
		)
		require.EqualError(t, err, "handler must be not nil")
	})

	main.Run("ErroredIfSourceNotSet", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		_, err := consumer.New(
			connCh,
			consumer.WithHandler(h),
		)
		require.EqualError(t, err, "WithQueue or WithExchange or WithDeclareQueue or WithTmpQueue options must be set")
	})

	main.Run("DeclareTemporaryQueueIfWithExchange", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume(any(), any(), any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().
			Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			QueueDeclare(any(), any(), any(), any(), any(), any()).
			Return(amqp.Queue{Name: "theTmpQueue"}, nil).
			Times(1)
		ch.EXPECT().
			QueueBind("theTmpQueue", "theKey", "theExchange", any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithExchange("theExchange", "theKey"),
		)
		require.NoError(t, err)

		assertReady(t, stateCh, "theTmpQueue")

		c.Close()
		assertClosed(t, c)
	})

	main.Run("WithTemporaryQueueResetOtherSources", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume("theTmpQueue", any(), any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().
			Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			QueueDeclare("", false, true, false, false, nil).
			Return(amqp.Queue{Name: "theTmpQueue"}, nil).
			Times(1)
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		connCh := make(chan *consumer.Connection, 1)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithExchange("aExchange", "aKey"),
			consumer.WithQueue("aQueue"),
			consumer.WithDeclareQueue("aQueue", true, true, true, true, amqp.Table{}),

			consumer.WithTmpQueue(),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theTmpQueue")

		c.Close()
		assertClosed(t, c)
	})

	main.Run("WithExchangeResetOtherSources", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume("theTmpQueue", any(), any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().
			Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			QueueDeclare("", false, true, false, false, nil).
			Return(amqp.Queue{Name: "theTmpQueue"}, nil).
			Times(1)
		ch.EXPECT().
			QueueBind("theTmpQueue", "theRoutingKey", "theExchange", false, nil).
			Return(nil).
			Times(1)
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		connCh := make(chan *consumer.Connection, 1)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithQueue("aQueue"),
			consumer.WithDeclareQueue("aQueue", true, true, true, true, amqp.Table{}),
			consumer.WithTmpQueue(),

			consumer.WithExchange("theExchange", "theRoutingKey"),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theTmpQueue")

		c.Close()
		assertClosed(t, c)
	})

	main.Run("WithQueueResetOtherSources", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume("theQueue", any(), any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().
			Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		connCh := make(chan *consumer.Connection, 1)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithDeclareQueue("aQueue", true, true, true, true, amqp.Table{}),
			consumer.WithTmpQueue(),
			consumer.WithExchange("theExchange", "theRoutingKey"),

			consumer.WithQueue("theQueue"),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theQueue")

		c.Close()
		assertClosed(t, c)
	})

	main.Run("WithDeclareQueueResetOtherSources", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		h := handlerStub(logger.NewTest())

		conn := mock_consumer.NewMockAMQPConnection(ctrl)
		ch := mock_consumer.NewMockAMQPChannel(ctrl)

		ch.EXPECT().
			Consume("theDeclareQueue", any(), any(), any(), any(), any(), any()).
			AnyTimes()
		ch.EXPECT().
			QueueDeclare("theDeclareQueue", true, true, true, true, amqp.Table{"theHeaderKey": "theHeaderValue"}).
			Return(amqp.Queue{Name: "theDeclareQueue"}, nil).
			Times(1)
		ch.EXPECT().
			Qos(any(), any(), any()).
			Times(1)
		ch.EXPECT().
			NotifyCancel(any()).
			AnyTimes()
		ch.EXPECT().
			NotifyClose(any()).
			AnyTimes()
		ch.EXPECT().
			Close().
			AnyTimes()

		connCh := make(chan *consumer.Connection, 1)

		stateCh := make(chan consumer.State, 2)

		c, err := consumer.New(
			connCh,
			consumer.WithInitFunc(initFuncStub(ch)),
			consumer.WithHandler(h),
			consumer.WithNotify(stateCh),
			consumer.WithTmpQueue(),
			consumer.WithExchange("theExchange", "theRoutingKey"),
			consumer.WithQueue("theQueue"),
			consumer.WithDeclareQueue("theDeclareQueue", true, true, true, true, amqp.Table{"theHeaderKey": "theHeaderValue"}),
		)
		require.NoError(t, err)

		connCh <- consumer.NewConnection(conn, nil)
		assertReady(t, stateCh, "theDeclareQueue")

		c.Close()
		assertClosed(t, c)
	})
}

func assertUnready(t *testing.T, stateCh <-chan consumer.State, errString string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state, ok := <-stateCh:
		if !ok {
			require.Equal(t, "permanently closed", errString)
			return
		}

		require.Nil(t, state.Ready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.EqualError(t, state.Unready.Err, errString)
	case <-timer.C:
		t.Fatal("consumer must be unready")
	}
}

func assertReady(t *testing.T, stateCh <-chan consumer.State, queue string) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case state, ok := <-stateCh:
		if !ok {
			require.Equal(t, "permanently closed", queue)
			return
		}

		require.Nil(t, state.Unready, fmt.Sprintf("%+v", state))

		require.NotNil(t, state.Ready, fmt.Sprintf("%+v", state))

		require.Equal(t, state.Ready.Queue, queue)

	case <-timer.C:
		t.Fatal("consumer must be ready")
	}
}

func assertClosed(t *testing.T, c *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.NotifyClosed():
	case <-timer.C:
		t.Fatal("consumer close timeout")
	}
}

func any() gomock.Matcher {
	return gomock.Any()
}

func handlerStub(l logger.Logger) consumer.Handler {
	return consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		l.Printf("[TEST] got message")
		return nil
	})
}

func handlerCounter(counter *int) consumer.Handler {
	return consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		time.Sleep(time.Millisecond * 10)
		*counter++
		return nil
	})
}

func initFuncStub(chs ...consumer.AMQPChannel) func(consumer.AMQPConnection) (consumer.AMQPChannel, error) {
	index := 0
	return func(_ consumer.AMQPConnection) (consumer.AMQPChannel, error) {
		currCh := chs[index]
		index++
		return currCh, nil
	}
}
