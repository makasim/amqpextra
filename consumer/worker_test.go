package consumer_test

import (
	"testing"

	"context"

	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDefaultWorkerProcessSomeMessages(t *testing.T) {
	goleak.VerifyNone(t)

	l := logger.NewTest()

	w := consumer.DefaultWorker{Logger: l}

	h := consumer.HandlerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		l.Printf("handler: %s", msg.Body)
		return nil
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	doneCh := make(chan struct{})

	msgCh := make(chan amqp.Delivery)
	go func() {
		defer close(doneCh)
		w.Serve(ctx, h, msgCh)
	}()

	msgCh <- amqp.Delivery{Body: []byte("first")}
	msgCh <- amqp.Delivery{Body: []byte("second")}
	msgCh <- amqp.Delivery{Body: []byte("third")}

	cancelFunc()
	<-doneCh

	require.Equal(t, `[DEBUG] worker starting
[INFO] handler: first
[INFO] handler: second
[INFO] handler: third
[DEBUG] worker stopped
`, l.Logs())
}

func TestDefaultWorkerHandlerReturnNotNil(t *testing.T) {
	goleak.VerifyNone(t)

	l := logger.NewTest()

	w := consumer.DefaultWorker{Logger: l}

	h := consumer.HandlerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		l.Printf("handler: %s", msg.Body)
		return "someValue"
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	doneCh := make(chan struct{})

	msgCh := make(chan amqp.Delivery)
	go func() {
		defer close(doneCh)
		w.Serve(ctx, h, msgCh)
	}()

	msgCh <- amqp.Delivery{Body: []byte("first")}

	cancelFunc()
	<-doneCh

	require.Equal(t, `[DEBUG] worker starting
[INFO] handler: first
[ERROR] handler return non nil result: "someValue"
[DEBUG] worker stopped
`, l.Logs())
}

func TestParallelWorkerProcessSomeMessages(t *testing.T) {
	goleak.VerifyNone(t)

	l := logger.NewTest()

	w := consumer.NewParallelWorker(5)
	w.Logger = l

	h := consumer.HandlerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		l.Printf("handler: %s", msg.Body)
		return nil
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	doneCh := make(chan struct{})

	msgCh := make(chan amqp.Delivery)
	go func() {
		defer close(doneCh)
		w.Serve(ctx, h, msgCh)
	}()

	msgCh <- amqp.Delivery{Body: []byte("123")}
	msgCh <- amqp.Delivery{Body: []byte("123")}
	msgCh <- amqp.Delivery{Body: []byte("123")}

	cancelFunc()
	<-doneCh

	require.Equal(t, `[DEBUG] worker starting
[INFO] handler: 123
[INFO] handler: 123
[INFO] handler: 123
[DEBUG] worker stopped
`, l.Logs())
}

func TestParallelWorkerHandlerReturnNotNil(t *testing.T) {
	goleak.VerifyNone(t)

	l := logger.NewTest()

	w := consumer.NewParallelWorker(5)
	w.Logger = l

	h := consumer.HandlerFunc(func(_ context.Context, msg amqp.Delivery) interface{} {
		l.Printf("handler: %s", msg.Body)
		return "someValue"
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	doneCh := make(chan struct{})

	msgCh := make(chan amqp.Delivery)
	go func() {
		defer close(doneCh)
		w.Serve(ctx, h, msgCh)
	}()

	msgCh <- amqp.Delivery{Body: []byte("first")}

	cancelFunc()
	<-doneCh

	require.Equal(t, `[DEBUG] worker starting
[INFO] handler: first
[ERROR] handler return non nil result: "someValue"
[DEBUG] worker stopped
`, l.Logs())
}
