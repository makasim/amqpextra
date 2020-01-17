package logger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
)

type Logger struct {
	mx     *sync.RWMutex
	buf    *bytes.Buffer
	output bool
}

func New() *Logger {
	return &Logger{
		buf:    bytes.NewBuffer(make([]byte, 0)),
		mx:     &sync.RWMutex{},
		output: false,
	}
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	fmt.Fprintf(l.buf, format+"\n", args...)

	if l.output {
		log.Printf(format, args...)
	}
}

func (l *Logger) Output(b bool) {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.output = b
}

func (l *Logger) Logs() string {
	l.mx.RLock()
	defer l.mx.RUnlock()

	b, _ := ioutil.ReadAll(l.buf)

	return string(b)
}
