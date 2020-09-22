package amqpextra

import (
	"testing"

	"time"

	"github.com/makasim/amqpextra/logger"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNewPublisher(t *testing.T) {
	defer goleak.VerifyNone(t)

	amqpConnCh := make(chan *amqp.Connection)
	amqpConnCloseCh := make(chan *amqp.Error, 1)
	l := logger.NewTest()

	p := NewPublisher(amqpConnCh, amqpConnCloseCh, publisher.WithLogger(l))

	p.Close()
	<-p.Closed()

	require.Equal(t, `[DEBUG] publisher starting
[DEBUG] publisher stopped
`, l.Logs())
}

func TestProxyPublisherConn(main *testing.T) {
	main.Run("Reconnect", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error, 1)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error, 1)
		pubCloseCh := make(chan struct{})
		defer close(pubCloseCh)

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)

		amqpConn := new(amqp.Connection)
		amqpConnCh <- amqpConn
		actualConn, ok := <-connCh
		require.True(t, ok)
		require.IsType(t, &publisher.AMQP{}, actualConn)
		require.Same(t, amqpConn, actualConn.(*publisher.AMQP).Conn)

		amqpConnCloseCh <- amqp.ErrClosed

		amqpConn = new(amqp.Connection)
		amqpConnCh <- amqpConn
		actualConn, ok = <-connCh
		require.True(t, ok)
		require.IsType(t, &publisher.AMQP{}, actualConn)
		require.Same(t, amqpConn, actualConn.(*publisher.AMQP).Conn)
	})

	main.Run("CloseWhenWaitForConnection", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error, 1)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error, 1)
		pubCloseCh := make(chan struct{})

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)
		close(pubCloseCh)

		_, ok := <-connCh
		require.False(t, ok)

		_, ok = <-connCloseCh
		require.False(t, ok)
	})

	main.Run("CloseBeforeReadPublisherConnection", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error, 1)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error, 1)
		pubCloseCh := make(chan struct{})

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)

		amqpConn := new(amqp.Connection)
		amqpConnCh <- amqpConn

		close(pubCloseCh)

		_, ok := <-connCloseCh
		require.False(t, ok)

		_, ok = <-connCh
		require.False(t, ok)
	})

	main.Run("ConnCloseBeforeReadPublisherConnection", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error)
		pubCloseCh := make(chan struct{})
		defer close(pubCloseCh)

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)

		amqpConn := new(amqp.Connection)
		amqpConnCh <- amqpConn

		close(amqpConnCh)
		amqpConnCloseCh <- amqp.ErrClosed

		time.Sleep(time.Millisecond * 50)

		_, ok := <-connCloseCh
		require.False(t, ok)

		_, ok = <-connCh
		require.False(t, ok)
	})

	main.Run("CloseWhenWaitForError", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error, 1)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error, 1)
		pubCloseCh := make(chan struct{})

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)

		amqpConn := new(amqp.Connection)
		amqpConnCh <- amqpConn
		<-connCh

		close(pubCloseCh)

		_, ok := <-connCloseCh
		require.False(t, ok)

		_, ok = <-connCh
		require.False(t, ok)
	})

	main.Run("ConnCloseWhenWaitForError", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		amqpConnCh := make(chan *amqp.Connection)
		amqpConnCloseCh := make(chan *amqp.Error)
		connCh := make(chan publisher.Connection)
		connCloseCh := make(chan *amqp.Error)
		pubCloseCh := make(chan struct{})
		defer close(pubCloseCh)

		go proxyPublisherConn(amqpConnCh, amqpConnCloseCh, connCh, connCloseCh, pubCloseCh)

		amqpConn := new(amqp.Connection)
		amqpConnCh <- amqpConn
		<-connCh

		close(amqpConnCh)
		amqpConnCloseCh <- amqp.ErrClosed

		time.Sleep(time.Millisecond * 50)

		_, ok := <-connCloseCh
		require.False(t, ok)

		_, ok = <-connCh
		require.False(t, ok)
	})
}
