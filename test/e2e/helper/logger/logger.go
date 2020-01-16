package logger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
)

type Logger struct {
	mx  *sync.RWMutex
	buf *bytes.Buffer
}

func New() *Logger {
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
