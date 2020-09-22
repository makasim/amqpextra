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

type defaultWorker struct {
	logger logger.Logger
}

func (dw *defaultWorker) Serve(ctx context.Context, h Handler, msgCh <-chan amqp.Delivery) {
	defer dw.logger.Printf("[DEBUG] worker stopped")

	dw.logger.Printf("[DEBUG] worker started")
	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return
			}

			if res := h.Handle(ctx, msg); res != nil {
				dw.logger.Printf("[ERROR] worker.serveMsg: non nil result: %#v", res)
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
	return &ParallelWorker{
		Num:    num,
		Logger: logger.Discard,
	}
}

func (pw *ParallelWorker) Serve(ctx context.Context, h Handler, msgCh <-chan amqp.Delivery) {
	defer pw.Logger.Printf("[DEBUG] workers stopped")

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
						pw.Logger.Printf("[ERROR] worker.serveMsg: non nil result: %#v", res)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	pw.Logger.Printf("[DEBUG] workers started")
	wg.Wait()
}
