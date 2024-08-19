package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"time"
)

func Republish(ctx context.Context, client *pubsub.Client, pubsubTopic string, pubsubSubscription string, maxMessages int) error {
	ch := make(chan pubsub.Message)
	eg := new(errgroup.Group)

	eg.Go(func() error {
		err := Receive(ctx, client, pubsubSubscription, func(c context.Context, m *pubsub.Message) {
			ch <- *m
		}, WithTimeout(5*time.Second), WithMaxMessages(maxMessages))
		close(ch)
		if err != nil {
			return fmt.Errorf("ReceiveN: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := Publish(ctx, client, pubsubTopic, func() (pubsub.Message, error) {
			m, ok := <-ch
			if !ok {
				return pubsub.Message{}, io.EOF
			}
			defer m.Ack()

			return m, nil
		}, WithPublishStatsLogging(os.Stdout, time.Second))
		if err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
		return nil
	})

	return eg.Wait()
}
