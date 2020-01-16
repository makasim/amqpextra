package standalone_consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/makasim/amqpextra/test/e2e/helper/rabbitmq"

	"github.com/makasim/amqpextra"

	"github.com/streadway/amqp"

	"github.com/makasim/amqpextra/test/e2e/helper/logger"

	"github.com/stretchr/testify/assert"
)

func TestCloseConsumerWhenConnChannelClosed(t *testing.T) {
	l := logger.New()

	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/amqpextra")
	assert.NoError(t, err)

	connCh := make(chan *amqp.Connection, 1)
	connCh <- conn

	closeCh := make(chan *amqp.Error)

	worker := amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
		return nil
	})

	go func() {
		<-time.NewTimer(time.Second).C

		close(closeCh)
		close(connCh)
	}()

	c := amqpextra.NewConsumer(rabbitmq.Queue(conn), worker, connCh, closeCh)
	c.SetLogger(l)

	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	assert.Equal(t, expected, l.Logs())
}

func TestCloseChannelOnAlreadyClosedConnection(t *testing.T) {
	l := logger.New()

	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})

	worker := amqpextra.WorkerFunc(func(msg amqp.Delivery, ctx context.Context) interface{} {
		return nil
	})

	go func() {
		<-time.NewTimer(time.Second).C

		conn.Close()
	}()

	c := conn.Consumer(rabbitmq.Queue2(conn), worker)
	c.SetLogger(l)

	c.Run()

	expected := `[DEBUG] consumer starting
[DEBUG] workers started
[DEBUG] workers stopped
[DEBUG] consumer stopped
`
	assert.Equal(t, expected, l.Logs())

	assert.NotContains(t, l.Logs(), "Exception (504) Reason: \"channel/connection is not open\"\n[DEBUG] consumer stopped\n")
}

//
//func TestCloseConnByContext(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	ctx, cancel := context.WithCancel(context.Background())
//
//	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
//	conn.SetContext(ctx)
//
//	go func() {
//		<-time.NewTimer(time.Second).C
//
//		cancel()
//	}()
//
//	conn.SetLogger(l)
//
//	connCh, closeCh := conn.Get()
//
//	_, ok := <-connCh
//	assert.True(t, ok)
//
//	_, ok = <-closeCh
//	assert.False(t, ok)
//
//	_, ok = <-connCh
//	assert.False(t, ok)
//
//	expected := `[DEBUG] connection established
//[DEBUG] connection is closed
//`
//	assert.Equal(t, expected, l.Logs())
//}
//
//func TestReconnectIfClosedByUser(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
//	conn.SetLogger(l)
//
//	connCh, closeCh := conn.Get()
//
//	realconn, ok := <-connCh
//	assert.True(t, ok)
//
//	assert.NoError(t, realconn.Close())
//
//	err, ok := <-closeCh
//	assert.True(t, ok)
//	assert.EqualError(t, err, "Exception (504) Reason: \"channel/connection is not open\"")
//
//	_, ok = <-connCh
//	assert.True(t, ok)
//
//	expected := `[DEBUG] connection established
//[DEBUG] connection established
//`
//	assert.Equal(t, expected, l.Logs())
//}
//
//func TestReconnectIfClosedByServer(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())
//
//	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
//		Properties: amqp.Table{
//			"connection_name": connName,
//		},
//	})
//	defer conn.Close()
//
//	conn.SetLogger(l)
//
//	connCh, closeCh := conn.Get()
//
//	_, ok := <-connCh
//	assert.True(t, ok)
//
//	assertlog.WaitContainsOrFatal(t, amqpextra2.conns, connName, time.Second*5)
//
//	if !assert.True(t, amqpextra2.closeConn(connName)) {
//		return
//	}
//
//	err, ok := <-closeCh
//	assert.True(t, ok)
//	assert.EqualError(t, err, "Exception (320) Reason: \"CONNECTION_FORCED - Closed via management plugin\"")
//
//	_, ok = <-connCh
//	assert.True(t, ok)
//
//	expected := `[DEBUG] connection established
//[DEBUG] connection established
//`
//	assert.Equal(t, expected, l.Logs())
//}
//
//func TestNotReadingFromCloseCh(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
//	conn.SetLogger(l)
//
//	connCh, _ := conn.Get()
//
//	realconn, ok := <-connCh
//	assert.True(t, ok)
//
//	assert.NoError(t, realconn.Close())
//
//	time.Sleep(time.Millisecond * 100)
//	//<-closeCh
//
//	realconn, ok = <-connCh
//	assert.True(t, ok)
//
//	assert.NoError(t, realconn.Close())
//
//	//<-closeCh
//
//	expected := `[DEBUG] connection established
//[DEBUG] connection established
//`
//	assert.Equal(t, expected, l.Logs())
//}
//
//func TestConnPublishConsume(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	conn := amqpextra.Dial([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"})
//	defer conn.Close()
//
//	conn.SetLogger(l)
//
//	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())
//
//	connCh, closeCh := conn.Get()
//
//	select {
//	case conn, ok := <-connCh:
//		assert.True(t, ok)
//
//		ch, err := conn.Channel()
//		assert.NoError(t, err)
//
//		q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
//		assert.NoError(t, err)
//
//		err = ch.Publish("", queue, false, false, amqp.Publishing{
//			Body: []byte("testbdy"),
//		})
//		assert.NoError(t, err)
//
//		msgCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
//		assert.NoError(t, err)
//
//		msg, ok := <-msgCh
//		assert.True(t, ok)
//
//		assert.NoError(t, msg.Ack(false))
//		assert.Equal(t, "testbdy", string(msg.Body))
//
//		expected := `[DEBUG] connection established
//`
//		assert.Equal(t, expected, l.Logs())
//	case <-closeCh:
//		t.Fatalf("connection is closed")
//	}
//}
//
//func TestCongruentlyPublishConsumeWhileConnectionLost(t *testing.T) {
//	l := amqpextra2.newLogger()
//
//	connName := fmt.Sprintf("amqpextra-test-%d", time.Now().UnixNano())
//
//	conn := amqpextra.DialConfig([]string{"amqp://guest:guest@rabbitmq:5672/amqpextra"}, amqp.Config{
//		Properties: amqp.Table{
//			"connection_name": connName,
//		},
//	})
//	defer conn.Close()
//
//	conn.SetLogger(l)
//
//	var wg sync.WaitGroup
//
//	wg.Add(1)
//	go func(connName string, wg *sync.WaitGroup) {
//		defer wg.Done()
//
//		<-time.NewTimer(time.Second * 5).C
//
//		if !assert.True(t, amqpextra2.closeConn(connName)) {
//			return
//		}
//	}(connName, &wg)
//
//	queue := fmt.Sprintf("test-%d", time.Now().Nanosecond())
//
//	var countPublished uint32
//	for i := 0; i < 5; i++ {
//		wg.Add(1)
//		go func(extraconn *amqpextra.Connection, queue string, wg *sync.WaitGroup) {
//			defer wg.Done()
//
//			connCh, closeCh := extraconn.Get()
//
//			ticker := time.NewTicker(time.Millisecond * 100)
//			timer := time.NewTimer(time.Second * 10)
//
//		L1:
//			for conn := range connCh {
//				ch, err := conn.Channel()
//				assert.NoError(t, err)
//
//				_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
//				assert.NoError(t, err)
//
//				for {
//					select {
//					case <-ticker.C:
//						err := ch.Publish("", queue, false, false, amqp.Publishing{})
//
//						if err == nil {
//							atomic.AddUint32(&countPublished, 1)
//						}
//					case <-closeCh:
//						continue L1
//					case <-timer.C:
//						break L1
//					}
//				}
//
//			}
//
//		}(conn, queue, &wg)
//	}
//
//	var countConsumed uint32
//	for i := 0; i < 5; i++ {
//		wg.Add(1)
//		go func(extraconn *amqpextra.Connection, queue string, count *uint32, wg *sync.WaitGroup) {
//			defer wg.Done()
//
//			connCh, closeCh := extraconn.Get()
//
//			timer := time.NewTimer(time.Second * 11)
//
//		L1:
//			for conn := range connCh {
//				ch, err := conn.Channel()
//				assert.NoError(t, err)
//
//				_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
//				assert.NoError(t, err)
//
//				msgCh, err := ch.Consume(queue, "", false, false, false, false, nil)
//				assert.NoError(t, err)
//
//				for {
//					select {
//					case msg, ok := <-msgCh:
//						if !ok {
//							continue L1
//						}
//
//						msg.Ack(false)
//
//						atomic.AddUint32(count, 1)
//					case <-closeCh:
//						continue L1
//					case <-timer.C:
//						break L1
//					}
//				}
//
//			}
//		}(conn, queue, &countConsumed, &wg)
//	}
//
//	wg.Wait()
//
//	expected := `[DEBUG] connection established
//[DEBUG] connection established
//`
//	assert.Equal(t, expected, l.Logs())
//
//	assert.GreaterOrEqual(t, countPublished, uint32(200))
//	assert.LessOrEqual(t, countPublished, uint32(520))
//
//	assert.GreaterOrEqual(t, countConsumed, uint32(200))
//	assert.LessOrEqual(t, countConsumed, uint32(520))
//}
