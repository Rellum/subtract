package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)
import "golang.org/x/time/rate"

var ErrTimeout = errors.New("timeout")
var ErrMaxMessages = errors.New("max messages")

func Receive(ctx context.Context, client *pubsub.Client, pubsubSubscription string, receiver func(c context.Context, m *pubsub.Message), opts ...ReceiveOption) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	o := receiveOptions{
		timeout:       time.Second * 5,
		maxMessages:   nil,
		rateLimiter:   rate.NewLimiter(rate.Inf, 1),
		statsFunc:     func(Stats) {},
		statsInterval: 5 * time.Second,
	}
	for i := range opts {
		opts[i](&o)
	}

	var mu sync.Mutex
	var total int
	observe := func(isComplete bool) {
		mu.Lock()
		stats := Stats{
			IsComplete: false,
			Total:      total,
		}
		mu.Unlock()
		o.statsFunc(stats)
	}
	ticker := time.NewTicker(o.statsInterval)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				observe(false)
			}
		}
	}()
	defer observe(true)

	ch := make(chan pubsub.Message)
	errCh := make(chan error)
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
		if err != nil {
			errCh <- err
		}
	}()

	timer := time.NewTimer(o.timeout)
	defer timer.Stop()
	for {
		select {
		case m := <-ch:
			if !timer.Stop() {
				timer.Reset(o.timeout)
			}
			if o.maxMessages != nil && o.maxMessages.Add(-1) < 0 {
				return ErrMaxMessages
			}
			if cctx.Err() != nil {
				return cctx.Err()
			}
			for {
				reservation := o.rateLimiter.Reserve()
				if !reservation.OK() {
					// Limited to zero rate. Backoff and try again.
					reservation.Cancel()
					err := wait(cctx, time.Second)
					if err != nil {
						return err
					}
					continue
				}
				err := wait(cctx, reservation.Delay())
				if err != nil {
					return err
				}
				break
			}

			receiver(cctx, &m)
			mu.Lock()
			total++
			mu.Unlock()

		case <-timer.C:
			return ErrTimeout

		case <-cctx.Done():
			return cctx.Err()

		case err := <-errCh:
			return err
		}
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type ReceiveOption func(*receiveOptions)

type receiveOptions struct {
	timeout       time.Duration
	maxMessages   *atomic.Int64
	rateLimiter   *rate.Limiter
	statsInterval time.Duration
	statsFunc     func(Stats)
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
