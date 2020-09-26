package amqpextra

import (
	"crypto/tls"
	"fmt"

	"github.com/streadway/amqp"
)

type Dialer interface {
	Dial() (Connection, error)
}

func NewDialer(url string, config amqp.Config) Dialer {
	return DialerFunc(func() (Connection, error) {
		amqpConn, err := amqp.DialConfig(url, config)
		if err != nil {
			return nil, err
		}

		return &connection{
			amqpConn: amqpConn,
			closeCh:  amqpConn.NotifyClose(make(chan *amqp.Error, 1)),
		}, nil
	})
}

type DialerFunc func() (Connection, error)

func (f DialerFunc) Dial() (Connection, error) {
	return f()
}

func Dial(urls []string) *Connector {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		amqpConn, err := amqp.Dial(url)
		if err != nil {
			return nil, err
		}

		return &connection{
			amqpConn: amqpConn,
			closeCh:  amqpConn.NotifyClose(make(chan *amqp.Error, 1)),
		}, nil
	}))
}

func DialTLS(urls []string, amqps *tls.Config) *Connector {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		amqpConn, err := amqp.DialTLS(url, amqps)
		if err != nil {
			return nil, err
		}

		return &connection{
			amqpConn: amqpConn,
			closeCh:  amqpConn.NotifyClose(make(chan *amqp.Error, 1)),
		}, nil
	}))
}

func DialConfig(urls []string, config amqp.Config) *Connector {
	i := 0
	l := len(urls)

	return New(DialerFunc(func() (Connection, error) {
		if len(urls) == 0 {
			return nil, fmt.Errorf("urls empty")
		}

		url := urls[i]

		i = (i + 1) % l

		amqpConn, err := amqp.DialConfig(url, config)
		if err != nil {
			return nil, err
		}

		return &connection{
			amqpConn: amqpConn,
			closeCh:  amqpConn.NotifyClose(make(chan *amqp.Error, 1)),
		}, nil
	}))
}
