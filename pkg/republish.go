package pkg

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"time"
)

func Republish(ctx context.Context, gcpProject string, pubsubTopic string, pubsubSubscription string, maxMessages int, verbose bool) error {
	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		return err
	}

	topic := client.Topic(pubsubTopic)
	defer topic.Stop()

	return ReceiveN(ctx, client, pubsubSubscription, 5*time.Second, maxMessages, func(c context.Context, m *pubsub.Message) {
		defer m.Nack()

		if verbose {
			fmt.Println("received message", m.ID)
		}

		_, err := topic.Publish(c, m).Get(context.Background())
		if err != nil {
			m.Nack()
			fmt.Errorf("publish: %w", err)
			return
		}

		if verbose {
			fmt.Printf("published message %s.\n", m.ID)
		}

		m.Ack()
	})
}
