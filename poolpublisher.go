package amqpextra

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/streadway/amqp"
)

type PoolPublisher struct {
	pp []*Publisher

	cases []reflect.SelectCase

	ctxIndex   int
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewPoolPublisher(pp []*Publisher) *PoolPublisher {
	ctx, cancelFunc := context.WithCancel(context.Background())

	cases := make([]reflect.SelectCase, len(pp))
	for i, p := range pp {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.Ready())}
	}

	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})

	return &PoolPublisher{
		pp: pp,

		ctxIndex:   len(cases),
		cases:      cases,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

func (p *PoolPublisher) SetLogger(logger Logger) {
	for _, pp := range p.pp {
		pp.SetLogger(logger)
	}
}

func (p *PoolPublisher) SetContext(ctx context.Context) {
	p.ctx, p.cancelFunc = context.WithCancel(ctx)

	for _, pp := range p.pp {
		pp.SetContext(p.ctx)
	}
}

func (p *PoolPublisher) SetRestartSleep(d time.Duration) {
	for _, pp := range p.pp {
		pp.SetRestartSleep(d)
	}
}

func (p *PoolPublisher) SetInitFunc(f func(conn *amqp.Connection) (*amqp.Channel, error)) {
	for _, pp := range p.pp {
		pp.SetInitFunc(f)
	}
}

func (p *PoolPublisher) Start() {
	for _, pp := range p.pp {
		pp.Start()
	}
}

func (p *PoolPublisher) Run() {
	p.Start()

	<-p.ctx.Done()
}

func (p *PoolPublisher) Close() {
	p.cancelFunc()

	for _, pp := range p.pp {
		pp.Close()
	}
}

func (p *PoolPublisher) Publish(msg Publishing) {
	p.Start()

	log.Print(1)
	chosen, _, ok := reflect.Select(p.cases)
	log.Print(2)
	if chosen == p.ctxIndex || !ok {
		if msg.ResultCh != nil {
			go func() {
				msg.ResultCh <- fmt.Errorf("publisher stopped")
			}()
		}

		return
	}
	log.Print(3, chosen)

	p.pp[chosen].Publish(msg)
}
