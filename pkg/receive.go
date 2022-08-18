package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
)

func ReceiveN(ctx context.Context, client *pubsub.Client, pubsubSubscription string, maxMessages int, receiver func(c context.Context, m *pubsub.Message)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := countN(ctx, maxMessages)

	subscription := client.Subscription(pubsubSubscription)

	return subscription.Receive(ctx, func(c context.Context, m *pubsub.Message) {
		select {
		case _, more := <-ch:
			if !more {
				return
			}
			receiver(c, m)
		case <-ctx.Done():
			return
		}
	})
}

func countN(ctx context.Context, n int) <-chan struct{} {
	counter := make(chan struct{})
	go func() {
		for {
			n--
			if ctx.Err() != nil || n < 0 {
				close(counter)
				return
			}
			counter <- struct{}{}
		}
	}()
	return counter
}
