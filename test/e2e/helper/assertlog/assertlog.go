package assertlog

import (
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
)

type Source interface {
	Logs() string
}

type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
}

type Service struct {
	test   TestingT
	source func() string
	curr   func() string
	ticker *time.Ticker
}

func New(source func() string, t TestingT) *Service {
	return &Service{
		source: source,
		test:   t,
		curr:   source,
		ticker: time.NewTicker(time.Microsecond * 100),
	}
}

func (s *Service) FromNow() *Service {
	offset := len(s.curr())

	return &Service{
		source: s.curr,
		test:   s.test,
		curr: func() string {
			return s.curr()[offset:]
		},
		ticker: time.NewTicker(time.Microsecond * 100),
	}
}

func (s *Service) Filter(f string) *Service {
	return &Service{
		source: s.curr,
		test:   s.test,
		ticker: time.NewTicker(time.Microsecond * 100),
		curr: func() string {
			var filtered []string

			lines := strings.Split(s.curr(), "\n")
			for _, line := range lines {
				if strings.Contains(line, f) {
					filtered = append(filtered, line)
				}
			}

			return strings.Join(filtered, "\n")
		},
	}
}

func (s *Service) WaitContains(expected string, timeout time.Duration) bool {
	timer := time.NewTicker(timeout)
	for {
		select {
		case <-timer.C:
			return assert.Contains(s.test, s.curr(), expected, s.source())
		case <-s.ticker.C:
			if strings.Contains(s.curr(), expected) {
				return true
			}
		}
	}
}

func (s *Service) WaitNotContains(expected string, timeout time.Duration) bool {
	timer := time.NewTicker(timeout)
	for {
		select {
		case <-timer.C:
			if assert.Contains(s.test, s.curr(), expected, s.source()) {
				return false
			}
		case <-s.ticker.C:
			return !strings.Contains(s.curr(), expected)
		}
	}
}

func (s *Service) WaitContainsOrFatal(expected string, timeout time.Duration) {
	if !s.WaitContains(expected, timeout) {
		s.test.FailNow()
	}
}

func (s *Service) WaitNotContainsOrFatal(expected string, timeout time.Duration) {
	if !s.WaitNotContains(expected, timeout) {
		s.test.FailNow()
	}
}

func (s *Service) NoErrors() bool {
	if !assert.NotContains(s.test, s.curr(), "level=error", "failed assert that curr does not contain \"level=error\" word") {
		return false
	}

	return s.NoPanicAndRace()
}

func (s *Service) NoPanic() bool {
	return assert.NotContains(s.test, s.curr(), "panic", "failed assert that curr does not contain panic word")
}

func (s *Service) NoRace() bool {
	return assert.NotContains(s.test, s.curr(), "DATA RACE", "failed assert that curr does not contain \"DATA RACE\" word")
}

func (s *Service) NoPanicAndRace() bool {
	if !s.NoPanic() {
		return false
	}

	if !s.NoRace() {
		return false
	}

	return true
}

func (s *Service) Logs() string {
	return s.curr()
}

func WaitContainsOrFatal(t TestingT, s func() string, expected string, timeout time.Duration) {
	New(s, t).WaitContainsOrFatal(expected, timeout)
}

func WaitContains(t TestingT, s func() string, expected string, timeout time.Duration) bool {
	return New(s, t).WaitContains(expected, timeout)
}

func WaitNotContains(t TestingT, s func() string, expected string, timeout time.Duration) bool {
	return New(s, t).WaitNotContains(expected, timeout)
}

func WaitNotContainsOrFatal(t TestingT, s func() string, expected string, timeout time.Duration) {
	New(s, t).WaitNotContainsOrFatal(expected, timeout)
}
