package consumer

import (
	"context"
	"go.uber.org/goleak"
	"testing"
	"time"
)

func TestRetryCounter(main *testing.T) {
	main.Run("Is initially zero", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rc := newRetryCounter(ctx, make(chan State))

		assertRetryCount(t, rc, 0)
	})

	main.Run("Is one when unready once", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan State)
		rc := newRetryCounter(ctx, ch)

		ch <- State{
			Unready: &Unready{},
		}

		assertRetryCount(t, rc, 1)
	})

	main.Run("Is two when unready twice", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan State)
		rc := newRetryCounter(ctx, ch)

		ch <- State{
			Unready: &Unready{},
		}
		ch <- State{
			Unready: &Unready{},
		}

		assertRetryCount(t, rc, 2)
	})

	main.Run("Is zero when incremented and reset", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan State)
		rc := newRetryCounter(ctx, ch)

		ch <- State{
			Unready: &Unready{},
		}
		ch <- State{
			Ready: &Ready{},
		}

		assertRetryCount(t, rc, 0)
	})
}

func assertRetryCount(t *testing.T, rc *retryCounter, expectedRetryCount int) {
	timer := time.NewTimer(time.Millisecond * 10)
	var lastActual int
	for {
		select {
		case <-timer.C:
			t.Errorf("retryCounter did not end up with correct value: %d, actual value: %d", expectedRetryCount, lastActual)
			return
		default:
			actual := rc.read()
			if expectedRetryCount != actual {
				lastActual = actual
				continue
			}

			if expectedRetryCount == actual {
				return
			}
		}
	}
}
