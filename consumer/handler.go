package consumer

import (
	"context"

	"github.com/streadway/amqp"
)

type Middleware func(next Handler) Handler

type Handler interface {
	Handle(ctx context.Context, msg amqp.Delivery) interface{}
}

type HandlerFunc func(ctx context.Context, msg amqp.Delivery) interface{}

func (f HandlerFunc) Handle(ctx context.Context, msg amqp.Delivery) interface{} {
	return f(ctx, msg)
}

func Wrap(handler Handler, middlewares ...Middleware) Handler {
	// Return ahead of time if there aren't any middlewares for the chain
	if len(middlewares) == 0 {
		return handler
	}

	// Wrap the end handler with the middleware chain
	w := middlewares[len(middlewares)-1](handler)
	for i := len(middlewares) - 2; i >= 0; i-- {
		w = middlewares[i](w)
	}

	return w
}
