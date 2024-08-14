package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)
import "golang.org/x/time/rate"

var ErrTimeout = errors.New("timeout")
var ErrMaxMessages = errors.New("max messages")

func Receive(ctx context.Context, client *pubsub.Client, pubsubSubscription string, receiver func(c context.Context, m *pubsub.Message), opts ...func(*receiveOptions)) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	o := receiveOptions{
		timeout:     time.Second * 5,
		maxMessages: nil,
		rateLimiter: rate.NewLimiter(rate.Inf, 1),
	}
	for i := range opts {
		opts[i](&o)
	}

	ch := make(chan pubsub.Message)
	subscription := client.Subscription(pubsubSubscription)
	go func() {
		err := subscription.Receive(cctx, func(c context.Context, m *pubsub.Message) {
			select {
			case <-c.Done():
				return
			case ch <- *m:
				return
			}
		})
		fmt.Printf("pubsub.Client.Subscription.Receive error: %v\n", err)
	}()

	timer := time.NewTimer(o.timeout)
	for {
		select {
		case m := <-ch:
			if !timer.Stop() {
				timer.Reset(o.timeout)
			}
			if o.maxMessages != nil && o.maxMessages.Add(-1) < 0 {
				cancel()
				return ErrMaxMessages
			}
			if cctx.Err() != nil {
				return cctx.Err()
			}
			if err := o.rateLimiter.Wait(cctx); err != nil {
				return err
			}

			receiver(cctx, &m)

		case <-timer.C:
			cancel()
			return ErrTimeout

		case <-cctx.Done():
			timer.Stop()
			return cctx.Err()
		}
	}
}

type receiveOptions struct {
	timeout     time.Duration
	maxMessages *atomic.Int64
	rateLimiter *rate.Limiter
}

func WithTimeout(timeout time.Duration) func(*receiveOptions) {
	return func(o *receiveOptions) {
		o.timeout = timeout
	}
}

func WithMaxMessages(maxMessages int) func(*receiveOptions) {
	var m atomic.Int64
	m.Store(int64(maxMessages))
	return func(o *receiveOptions) {
		o.maxMessages = &m
	}
}

func WithRateLimiter(rateLimit *rate.Limiter) func(*receiveOptions) {
	return func(o *receiveOptions) {
		o.rateLimiter = rateLimit
	}
}
