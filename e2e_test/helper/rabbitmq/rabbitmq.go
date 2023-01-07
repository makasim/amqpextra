package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
				http.NoBody,
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
	req, err := http.NewRequest("GET", "http://guest:guest@rabbitmq:15672/api/connections", http.NoBody)
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

	b, err := io.ReadAll(resp.Body)
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

func Publish(conn *amqp.Connection, body, queue string) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	err = ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		Body: []byte(body),
	})
	if err != nil {
		panic(err)
	}
}
