package amqpextra

import (
	"crypto/tls"
	"fmt"

	"github.com/streadway/amqp"
)

type Dialer interface {
	Dial() (*amqp.Connection, error)
}

func NewDialer(url string, config amqp.Config) Dialer {
	return DialerFunc(func() (*amqp.Connection, error) {
		return amqp.DialConfig(url, config)
	})
}

type DialerFunc func() (*amqp.Connection, error)

func (f DialerFunc) Dial() (*amqp.Connection, error) {
	return f()
}

func Dial(urls []string) *Connection {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (*amqp.Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		return amqp.Dial(url)
	}))
}

func DialTLS(urls []string, amqps *tls.Config) *Connection {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (*amqp.Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		return amqp.DialTLS(url, amqps)
	}))
}

func DialConfig(urls []string, config amqp.Config) *Connection {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (*amqp.Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		return amqp.DialConfig(url, config)
	}))
}
