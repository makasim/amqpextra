package logger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
)

type TestLogger struct {
	mx     *sync.RWMutex
	buf    *bytes.Buffer
	output bool
}

func addLvlArg(lvl string, args ...interface{}) []interface{} {

	return append([]interface{}{"[" + lvl + "] "}, args...)
}

func (l *TestLogger) Info(args ...interface{}) {
	l.print(addLvlArg("INFO", args...)...)
}

func (l *TestLogger) Error(args ...interface{}) {
	l.print(addLvlArg("ERROR", args...)...)
}

func (l *TestLogger) Errorf(format string, args ...interface{}) {
	l.printf("[ERROR] "+format, args...)
}

func (l *TestLogger) Debug(args ...interface{}) {
	l.print(addLvlArg("DEBUG", args...)...)
}

func (l *TestLogger) Debugf(format string, args ...interface{}) {
	l.printf("[DEBUG] "+format, args...)
}

func (l *TestLogger) Warn(args ...interface{}) {
	l.print(addLvlArg("WARN", args...)...)
}

func (l *TestLogger) Warnf(format string, args ...interface{}) {
	l.printf("[WARN] "+format, args...)
}

func (l *TestLogger) Printf(format string, args ...interface{}) {
	l.printf("[INFO] "+format, args...)
}

func NewTest() *TestLogger {
	return &TestLogger{
		buf:    bytes.NewBuffer(make([]byte, 0)),
		mx:     &sync.RWMutex{},
		output: false,
	}
}

func (l *TestLogger) printf(format string, args ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()
	fmt.Fprintf(l.buf, format+"\n", args...)
	if l.output {
		log.Printf(format, args...)
	}
}

func (l *TestLogger) print(args ...interface{}) {
	l.mx.Lock()
	defer l.mx.Unlock()

	fmt.Fprint(l.buf, append(args, "\n")...)
	if l.output {
		log.Print(args...)
	}
}

func (l *TestLogger) Output(b bool) {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.output = b
}

func (l *TestLogger) Logs() string {
	l.mx.RLock()
	defer l.mx.RUnlock()

	b, _ := ioutil.ReadAll(l.buf)

	return string(b)
}
