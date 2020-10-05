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

func TestUnready(main *testing.T) {
	main.Run("CloseByMethod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(connCh, consumer.WithQueue("theQueue"), consumer.WithHandler(h), consumer.WithLogger(l))
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
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

		c, err := consumer.New(connCh, consumer.WithQueue("foo"), consumer.WithHandler(h), consumer.WithLogger(l), consumer.WithContext(ctx))
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
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

		c, err := consumer.New(connCh, consumer.WithQueue("foo"), consumer.WithHandler(h), consumer.WithLogger(l))
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
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

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
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

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
		close(connCh)
		time.Sleep(time.Millisecond * 220)
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[ERROR] init func: the error
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})

	main.Run("UnreadyWhileInitRetrySleep", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
		assertUnready(t, c, "the error")
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

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
		close(connCh)
		time.Sleep(time.Millisecond * 220)
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

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
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

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.AMQPConnection) (consumer.AMQPChannel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
		close(connCh)
		time.Sleep(time.Millisecond * 220)
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
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
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
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan *consumer.Connection, 1)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertUnready(t, c, amqp.ErrClosed.Error())
		connCh <- consumer.NewConnection(conn, nil)
		time.Sleep(time.Millisecond * 200)
		close(connCh)
		time.Sleep(time.Millisecond * 220)
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

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("foo"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume("theQueue", "", false, false, false, false, amqp.Table(nil)).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithConsumeArgs("theConsumer", true, true, true, true, table),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Times(1)

		amqpConn := mock_consumer.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := consumer.NewConnection(amqpConn, closeCh)
		connCh := make(chan *consumer.Connection, 1)
		connCh <- conn

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		assertReady(t, c)
		time.Sleep(time.Millisecond * 50)
		chCloseCh <- amqp.ErrClosed
		assertReady(t, c)
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

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		newChCloseCh := make(chan *amqp.Error)
		newCancelCh := make(chan string)
		newNsgCh := make(chan amqp.Delivery)

		newConn := mock_consumer.NewMockAMQPConnection(ctrl)

		newCh := mock_consumer.NewMockAMQPChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(newNsgCh, nil).Times(1)
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
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		assertReady(t, c)
		time.Sleep(time.Millisecond * 50)
		cancelCh <- "aTag"
		assertUnready(t, c, "consumption canceled")

		connCh <- consumer.NewConnection(newConn, nil)
		assertReady(t, c)

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
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 1)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(handlerStub(l)),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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

		ch := mock_consumer.NewMockAMQPChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
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
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		newConn := mock_consumer.NewMockAMQPConnection(ctrl)

		closeCh := make(chan struct{})
		conn := consumer.NewConnection(amqpConn, closeCh)
		connCh := make(chan *consumer.Connection, 2)
		connCh <- conn
		connCh <- consumer.NewConnection(newConn, nil)

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
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().NotifyCancel(any()).
			Return(cancelCh).Times(1)
		ch.EXPECT().Close().Return(amqp.ErrClosed).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 2)
		connCh <- consumer.NewConnection(conn, nil)

		c, err := consumer.New(
			connCh,
			consumer.WithQueue("theQueue"),
			consumer.WithHandler(h),
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch)),
		)
		require.NoError(t, err)

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

		assertReady(t, c)
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
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().NotifyCancel(any()).
			Return(newCancelCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockAMQPConnection(ctrl)

		connCh := make(chan *consumer.Connection, 2)
		connCh <- consumer.NewConnection(conn, nil)

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
			consumer.WithRetryPeriod(time.Millisecond),
			consumer.WithInitFunc(initFuncStub(ch, newCh)),
		)
		require.NoError(t, err)

		assertReady(t, c)
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

func assertUnready(t *testing.T, c *consumer.Consumer, errString string) {
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
		t.Fatal("publisher must be unready")
	}
}

func assertReady(t *testing.T, c *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.NotifyReady():
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
