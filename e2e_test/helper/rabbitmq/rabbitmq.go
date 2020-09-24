package rabbitmq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/makasim/amqpextra"

	"github.com/streadway/amqp"
)

func CloseConn(userProvidedName string) bool {
	defer http.DefaultClient.CloseIdleConnections()

	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(OpenedConns()), &data); err != nil {
		panic(err)
	}

	for _, conn := range data {
		connUserProvidedName, ok := conn["user_provided_name"].(string)
		if !ok {
			continue
		}

		if connUserProvidedName == userProvidedName {
			req, err := http.NewRequest(
				"DELETE",
				fmt.Sprintf("http://guest:guest@rabbitmq:15672/api/connections/%s", conn["name"].(string)),
				nil,
			)
			if err != nil {
				panic(err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				panic(err)
			}
			if err := resp.Body.Close(); err != nil {
				panic(err)
			}

			if resp.StatusCode != http.StatusNoContent {
				b, _ := httputil.DumpResponse(resp, true)

				panic(fmt.Sprintf("delete connection request failed:\n\n%s", string(b)))
			}

			return true
		}
	}

	return false
}

func IsOpened(userProvidedName string) bool {
	defer http.DefaultClient.CloseIdleConnections()

	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(OpenedConns()), &data); err != nil {
		panic(err)
	}

	for _, conn := range data {
		connUserProvidedName, ok := conn["user_provided_name"].(string)
		if !ok {
			continue
		}

		if connUserProvidedName == userProvidedName {
			return true
		}
	}

	return false
}

func OpenedConns() string {
	req, err := http.NewRequest("GET", "http://guest:guest@rabbitmq:15672/api/connections", nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := httputil.DumpResponse(resp, true)

		panic(fmt.Sprintf("get connections request failed:\n\n%s", string(b)))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func Queue(conn *amqp.Connection) string {
	queue := fmt.Sprintf("test-amqpextra-%d", time.Now().UnixNano())

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	return q.Name
}

func Queue2(conn *amqpextra.Connection) string {
	connCh, _ := conn.ConnCh()

	realconn, ok := <-connCh
	if !ok {
		panic("connection is closed")
	}

	return Queue(realconn)
}

func Publish(conn *amqp.Connection, body, queue string) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	err = ch.Publish("", queue, false, false, amqp.Publishing{
		Body: []byte(body),
	})
	if err != nil {
		panic(err)
	}
}

func Publish2(conn *amqpextra.Connection, body, queue string) {
	connCh, _ := conn.ConnCh()

	realconn, ok := <-connCh
	if !ok {
		panic("connection is closed")
	}

	Publish(realconn, body, queue)
}

func PublishTimerReconnect(
	extraconn *amqpextra.Connection,
	timer *time.Timer,
	ticker *time.Ticker,
	queue string,
	count *uint32,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer timer.Stop()
	defer ticker.Stop()

	connCh, closeCh := extraconn.ConnCh()

	for conn := range connCh {
		ch, err := conn.Channel()
		if err != nil {
			panic(err)
		}

		_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			panic(err)
		}

		for {
			select {
			case <-ticker.C:
				err := ch.Publish("", queue, false, false, amqp.Publishing{})

				if err == nil {
					atomic.AddUint32(count, 1)
				}
			case <-closeCh:
				break
			case <-timer.C:
				return
			}
		}
	}
}

func PublishTimer(
	conn *amqp.Connection,
	timer *time.Timer,
	ticker *time.Ticker,
	queue string,
	count *uint32,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer timer.Stop()
	defer ticker.Stop()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ticker.C:
			err := ch.Publish("", queue, false, false, amqp.Publishing{})

			if err == nil {
				atomic.AddUint32(count, 1)
			}
		case <-timer.C:
			return
		}
	}
}

func ConsumeTimerReconnect(
	extraconn *amqpextra.Connection,
	timer *time.Timer,
	queue string,
	count *uint32,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer timer.Stop()

	connCh, closeCh := extraconn.ConnCh()

	for conn := range connCh {
		ch, err := conn.Channel()
		if err != nil {
			panic(err)
		}

		_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			panic(err)
		}

		msgCh, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			panic(err)
		}

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					break
				}

				msg.Ack(false)

				atomic.AddUint32(count, 1)
			case <-closeCh:
				break
			case <-timer.C:
				return
			}
		}
	}
}

func ConsumeReconnect(
	conn *amqp.Connection,
	timer *time.Timer,
	queue string,
	count *uint32,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer timer.Stop()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	msgCh, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				break
			}

			msg.Ack(false)

			atomic.AddUint32(count, 1)
		case <-timer.C:
			return
		}
	}
}
