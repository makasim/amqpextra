package logrus

import (
	"strings"

	"github.com/sirupsen/logrus"
)

type Logger struct {
	l logrus.FieldLogger
}

func New(l logrus.FieldLogger) *Logger {
	return &Logger{
		l: l,
	}
}

func (l *Logger) Printf(format string, v ...interface{}) {
	ll := l.l
	if err := l.findError(v); err != nil {
		ll = ll.WithError(err)
	}

	if strings.HasPrefix(format, "[ERROR] ") {
		format = format[8:]

		ll.Errorf(format, v...)
	} else if strings.HasPrefix(format, "[WARN] ") {
		format = format[7:]

		ll.Warnf(format, v...)
	} else if strings.HasPrefix(format, "[DEBUG] ") {
		format = format[8:]

		ll.Debugf(format, v...)
	} else {
		ll.Infof(format, v...)
	}
}

func (l *Logger) findError(values []interface{}) error {
	for _, v := range values {
		if err, ok := v.(error); ok {
			return err
		}
	}

	return nil
}
