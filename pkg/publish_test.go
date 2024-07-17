package pkg_test

import (
	"bufio"
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/option"
	"io"
	"os"
	"runtime"
	"subtract/pkg"
	"testing"
	"time"
)

func TestPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	projectID := "test-project-id"
	const n int = 10000

	err := os.Setenv("PUBSUB_EMULATOR_HOST", ":8089")
	if err != nil {
		t.Error(err)
	}
	err = os.Setenv("PUBSUB_PROJECT_ID", projectID)
	if err != nil {
		t.Error(err)
	}

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConnectionPool(1))
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	topic, subscription := setupPubSub(t, ctx, client)
	defer topic.Stop()

	r, w := io.Pipe()
	scanner := bufio.NewScanner(r)
	go func() {
		timestamp := time.Now().Format(time.DateTime)
		for i := 0; i < n; i++ {
			_, err := fmt.Fprintf(w, "m_%s_%06d\n", timestamp, i)
			if err != nil {
				t.Error(err)
			}
		}
		w.Close()
	}()
	err = pkg.Publish(ctx, client, topic.ID(), pkg.ScanPayloads(scanner))
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), time.Second, func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestPublish_withLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	projectID := "test-project-id"
	const n int = 10000

	err := os.Setenv("PUBSUB_EMULATOR_HOST", ":8089")
	if err != nil {
		t.Error(err)
	}
	err = os.Setenv("PUBSUB_PROJECT_ID", projectID)
	if err != nil {
		t.Error(err)
	}

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConnectionPool(1))
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	topic, subscription := setupPubSub(t, ctx, client)
	defer topic.Stop()

	r, w := io.Pipe()
	scanner := bufio.NewScanner(r)
	go func() {
		timestamp := time.Now().Format(time.DateTime)
		for i := 0; i < n; i++ {
			time.Sleep(100 * time.Microsecond)
			_, err := fmt.Fprintf(w, "m_%s_%06d\n", timestamp, i)
			if err != nil {
				t.Error(err)
			}
		}
		w.Close()
	}()
	err = pkg.Publish(ctx, client, topic.ID(), pkg.ScanPayloads(scanner), pkg.WithStatsLogging(50*time.Millisecond))
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), time.Second, func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func setupPubSub(t *testing.T, ctx context.Context, client *pubsub.Client) (*pubsub.Topic, *pubsub.Subscription) {
	suffix := fmt.Sprintf("") //, time.Now().UnixMilli())
	topic := createTopic(t, ctx, client, "topic"+suffix)
	return topic, createSubscription(t, ctx, client, "subscription"+suffix, topic)
}

func createTopic(t *testing.T, ctx context.Context, client *pubsub.Client, name string) *pubsub.Topic {
	topic := client.Topic(name)
	if exists, _ := topic.Exists(ctx); exists {
		fmt.Println("topic already exists")
		return topic
	}

	topic, err := client.CreateTopic(ctx, name)
	if err != nil {
		t.Error(err)
	}

	return topic
}

func createSubscription(t *testing.T, ctx context.Context, client *pubsub.Client, name string, topic *pubsub.Topic) *pubsub.Subscription {
	if topic == nil {
		t.Error("topic cannot be nil when subscribing to it")
	}
	sub := client.Subscription(name)
	if exists, _ := sub.Exists(ctx); exists {
		fmt.Println("subscription already exists")
		return sub
	}

	sub, err := client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic:               topic,
		RetainAckedMessages: false,
		AckDeadline:         10 * time.Second,
		ExpirationPolicy:    10 * time.Second,
	})
	if err != nil {
		t.Error(err)
	}

	return sub
}

func mark() {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("%v: %v:%v\n", time.Now(), file, line)
}
