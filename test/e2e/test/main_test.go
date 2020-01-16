package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"sync"
)

type Logger struct {
	mx  *sync.RWMutex
	buf *bytes.Buffer
}

func newLogger() *Logger {
	return &Logger{
		buf: bytes.NewBuffer(make([]byte, 0)),
		mx:  &sync.RWMutex{},
	}
}

func (l Logger) Printf(format string, args ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	fmt.Fprintf(l.buf, format+"\n", args...)
}

func (l Logger) Logs() string {
	l.mx.RLock()
	defer l.mx.RUnlock()

	b, _ := ioutil.ReadAll(l.buf)

	return string(b)
}

func closeConn(userProvidedName string) bool {
	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(conns()), &data); err != nil {
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
			resp.Body.Close()

			if resp.StatusCode != http.StatusNoContent {
				b, _ := httputil.DumpResponse(resp, true)

				panic(fmt.Sprintf("delete connection request failed:\n\n%s", string(b)))
			}

			return true
		}
	}

	return false
}

func conns() string {
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
