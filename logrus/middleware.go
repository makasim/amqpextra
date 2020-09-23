package logrus

import (
	"context"

	"github.com/makasim/amqpextra/consumer/middleware"
	"github.com/sirupsen/logrus"
)

func WithLogger(ctx context.Context, l logrus.FieldLogger) context.Context {
	return middleware.WithLogger(ctx, New(l))
}

func GetLogger(ctx context.Context) (logrus.FieldLogger, bool) {
	l, ok := middleware.GetLogger(ctx)
	if !ok {
		return nil, false
	}

	ll, ok := l.(*Logger)
	if !ok {
		return nil, false
	}

	return ll.l, ok
}
