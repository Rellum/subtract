package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"time"
)

func Receive(ctx context.Context, client *pubsub.Client, pubsubSubscription string, timeout time.Duration, receiver func(c context.Context, m *pubsub.Message)) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan pubsub.Message, 100)
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
	for {
		timer := time.NewTimer(timeout)
		select {
		case m := <-ch:
			if !timer.Stop() {
				<-timer.C
			}
			receiver(cctx, &m)

		case <-timer.C:
			cancel()
			return cctx.Err()
		}
	}
}

func ReceiveN(ctx context.Context, client *pubsub.Client, pubsubSubscription string, timeout time.Duration, maxMessages int, receiver func(c context.Context, m *pubsub.Message)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var count int
	return Receive(ctx, client, pubsubSubscription, timeout, func(c context.Context, m *pubsub.Message) {
		count++
		if count > maxMessages {
			cancel()
			return
		}
		receiver(c, m)
	})
}
