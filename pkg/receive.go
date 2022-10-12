package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"sync"
	"time"
)

func ReceiveN(ctx context.Context, client *pubsub.Client, pubsubSubscription string, maxMessages int, receiver func(c context.Context, m *pubsub.Message)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := countN(ctx, maxMessages)

	subscription := client.Subscription(pubsubSubscription)

	var wg sync.WaitGroup
	var once sync.Once

	err := subscription.Receive(ctx, func(c context.Context, m *pubsub.Message) {
		wg.Add(1)
		_, more := <-ch
		if !more {
			wg.Done()
			once.Do(func() {
				wg.Wait()
				cancel()
			})
			<-c.Done()
			m.Nack()
			return
		}

		receiver(c, m)
		wg.Done()
	})
	if !errors.Is(err, ctx.Err()) {
		return err
	}

	return nil
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
			select {
			case counter <- struct{}{}:
			case <-time.After(time.Second * 5):
				close(counter)
				return
			}
		}
	}()
	return counter
}
