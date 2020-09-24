package consumer

import (
	"context"
	"sync"

	"github.com/makasim/amqpextra/logger"
	"github.com/streadway/amqp"
)

type Worker interface {
	Serve(ctx context.Context, h Handler, msgCh <-chan amqp.Delivery)
}

type DefaultWorker struct {
	Logger logger.Logger
}

func (dw *DefaultWorker) Serve(ctx context.Context, h Handler, msgCh <-chan amqp.Delivery) {
	defer dw.Logger.Printf("[DEBUG] worker stopped")

	dw.Logger.Printf("[DEBUG] worker starting")
	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return
			}

			if res := h.Handle(ctx, msg); res != nil {
				dw.Logger.Printf("[ERROR] handler return non nil result: %#v", res)
			}
		case <-ctx.Done():
			return
		}
	}
}

type ParallelWorker struct {
	Num    int
	Logger logger.Logger
}

func NewParallelWorker(num int) *ParallelWorker {
	if num < 1 {
		panic("num workers must be greater than zero")
	}

	return &ParallelWorker{
		Num:    num,
		Logger: logger.Discard,
	}
}

func (pw *ParallelWorker) Serve(ctx context.Context, h Handler, msgCh <-chan amqp.Delivery) {
	defer pw.Logger.Printf("[DEBUG] worker stopped")

	pw.Logger.Printf("[DEBUG] worker starting")

	wg := &sync.WaitGroup{}
	for i := 0; i < pw.Num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case msg, ok := <-msgCh:
					if !ok {
						return
					}

					if res := h.Handle(ctx, msg); res != nil {
						pw.Logger.Printf("[ERROR] handler return non nil result: %#v", res)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
}
