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
	"go.uber.org/goleak"
)

func TestUnready(main *testing.T) {
	main.Run("CloseByMethod", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New("foo", h, connCh, connCloseCh, consumer.WithLogger(l))
		go c.Run()

		assertUnready(t, c)
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

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New("foo", h, connCh, connCloseCh, consumer.WithLogger(l), consumer.WithContext(ctx))
		go c.Run()

		assertUnready(t, c)
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

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New("foo", h, connCh, connCloseCh, consumer.WithLogger(l))
		go c.Run()

		assertUnready(t, c)
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

		conn := mock_consumer.NewMockConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.Connection) (consumer.Channel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		conn := mock_consumer.NewMockConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.Connection) (consumer.Channel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		conn := mock_consumer.NewMockConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.Connection) (consumer.Channel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
		time.Sleep(time.Millisecond * 200)
		assertUnready(t, c)
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

		conn := mock_consumer.NewMockConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.Connection) (consumer.Channel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(nil, fmt.Errorf("the error")).Times(1)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		conn := mock_consumer.NewMockConnection(ctrl)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
			consumer.WithInitFunc(func(conn consumer.Connection) (consumer.Channel, error) {
				return nil, fmt.Errorf("the error")
			}),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(nil, fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		l := logger.NewTest()
		h := handlerStub(l)

		connCh := make(chan consumer.Connection, 1)
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond*400),
		)
		go c.Run()

		assertUnready(t, c)
		connCh <- conn
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"foo",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
		)
		go c.Run()

		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume("theQueue", "", false, false, false, false, amqp.Table(nil)).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l))
		go c.Run()

		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		table := amqp.Table{"foo": "fooVal"}

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume("theQueue", "theConsumer", true, true, true, true, table).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithConsumeArgs("theConsumer", true, true, true, true, table))
		go c.Run()

		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().Return(ch, nil).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l))
		go c.Run()

		assertReady(t, c)
		close(connCh)
		connCloseCh <- amqp.ErrClosed
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		newChCloseCh := make(chan *amqp.Error)
		newNsgCh := make(chan amqp.Delivery)

		newCh := mock_consumer.NewMockChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(newNsgCh, nil).Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().Close().Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch, newCh)).Times(2)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		time.Sleep(time.Millisecond * 50)
		chCloseCh <- amqp.ErrClosed
		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
[DEBUG] channel closed
[DEBUG] worker stopped
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(fmt.Errorf("the error")).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch)).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(amqp.ErrClosed).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch)).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch)).Times(1)

		connCh := make(chan consumer.Connection, 1)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			handlerStub(l),
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		msgCh <- amqp.Delivery{}
		msgCh <- amqp.Delivery{}
		msgCh <- amqp.Delivery{}
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch)).Times(1)

		newChCloseCh := make(chan *amqp.Error)
		newCh := mock_consumer.NewMockChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		newConn := mock_consumer.NewMockConnection(ctrl)
		newConn.EXPECT().Channel().DoAndReturn(ChannelStub(newCh)).Times(1)

		connCh := make(chan consumer.Connection, 2)
		connCh <- conn
		connCh <- newConn
		connCloseCh := make(chan *amqp.Error, 1)

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

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		time.Sleep(time.Millisecond * 300)
		connCloseCh <- amqp.ErrClosed
		wg.Wait()
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, 100, countConsumed)
		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer ready
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(amqp.ErrClosed).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch)).Times(1)

		connCh := make(chan consumer.Connection, 2)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		wg := &sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					select {
					case msgCh <- amqp.Delivery{}:
					case <-c.Closed():
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
[DEBUG] worker started
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
		msgCh := make(chan amqp.Delivery)

		ch := mock_consumer.NewMockChannel(ctrl)
		ch.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		ch.EXPECT().NotifyClose(any()).
			Return(chCloseCh).Times(1)
		ch.EXPECT().Close().Return(nil).Times(1)

		newChCloseCh := make(chan *amqp.Error)
		newCh := mock_consumer.NewMockChannel(ctrl)
		newCh.EXPECT().Consume(any(), any(), any(), any(), any(), any(), any()).
			Return(msgCh, nil).Times(1)
		newCh.EXPECT().NotifyClose(any()).
			Return(newChCloseCh).Times(1)
		newCh.EXPECT().Close().Return(nil).Times(1)

		conn := mock_consumer.NewMockConnection(ctrl)
		conn.EXPECT().Channel().DoAndReturn(ChannelStub(ch, newCh)).Times(2)

		connCh := make(chan consumer.Connection, 2)
		connCh <- conn
		connCloseCh := make(chan *amqp.Error, 1)

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

		c := consumer.New(
			"theQueue",
			h,
			connCh,
			connCloseCh,
			consumer.WithLogger(l),
			consumer.WithRetryPeriod(time.Millisecond),
		)
		go c.Run()

		assertReady(t, c)
		time.Sleep(time.Millisecond * 300)
		chCloseCh <- amqp.ErrClosed
		wg.Wait()
		c.Close()
		assertClosed(t, c)

		assert.Equal(t, 100, countConsumed)
		assert.Equal(t, `[DEBUG] consumer starting
[DEBUG] consumer ready
[DEBUG] worker started
[DEBUG] channel closed
[DEBUG] worker stopped
[DEBUG] consumer ready
[DEBUG] worker started
[DEBUG] worker stopped
[DEBUG] consumer unready
[DEBUG] consumer stopped
`, l.Logs())
	})
}

func assertUnready(t *testing.T, c *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.Unready():
	case <-timer.C:
		t.Fatal("publisher must be unready")
	}
}

func assertReady(t *testing.T, c *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.Ready():
	case <-timer.C:
		t.Fatal("consumer must be ready")
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

func assertClosed(t *testing.T, c *consumer.Consumer) {
	timer := time.NewTimer(time.Millisecond * 100)
	defer timer.Stop()

	select {
	case <-c.Closed():
	case <-timer.C:
		t.Fatal("consumer close timeout")
	}
}

func ChannelStub(chs ...consumer.Channel) func() (consumer.Channel, error) {
	index := 0
	return func() (consumer.Channel, error) {
		currCh := chs[index]
		index++
		return currCh, nil
	}
}