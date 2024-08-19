package pkg_test

import (
	"bufio"
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"math/rand"
	"regexp"
	"runtime"
	"subtract/pkg"
	"testing"
	"time"
)

const projectID = "test-project-id"

func TestPublish(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	var count int
	nextFn := func() (pubsub.Message, error) {
		i := count
		count++
		if i >= n {
			return pubsub.Message{}, io.EOF
		}

		return pubsub.Message{
			Data: []byte(fmt.Sprintf("m_%06d\n", i)),
		}, nil
	}
	err := pkg.Publish(ctx, client, topic.ID(), nextFn)
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestPublish_ScanPayloads(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	r, w := io.Pipe()
	scanner := bufio.NewScanner(r)
	go func() {
		for i := 0; i < n; i++ {
			_, err := fmt.Fprintf(w, "m_%06d\n", i)
			if err != nil {
				t.Error(err)
			}
		}
		w.Close()
	}()
	err := pkg.Publish(ctx, client, topic.ID(), pkg.ScanPayloads(scanner))
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestPublish_ScanMessages(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	r, w := io.Pipe()
	scanner := bufio.NewScanner(r)
	go func() {
		for i := 0; i < n; i++ {
			_, err := fmt.Fprintf(w, "{\"ID\":\"%d\",\"Data\":\"%s\",\"Attributes\":{\"Baz\":\"m_%06d\",\"Foo\":\"Bar\"},\"PublishTime\":\"2024-07-17T13:06:51.026Z\",\"DeliveryAttempt\":null,\"OrderingKey\":\"\"}\n", i, base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("m_%06d", i))), i)
			if err != nil {
				t.Error(err)
			}
		}
		w.Close()
	}()
	err := pkg.Publish(ctx, client, topic.ID(), pkg.ScanMessages(scanner))
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		if m.Attributes["Foo"] != "Bar" {
			t.Errorf("attribute 'Foo' incorrect")
		}
		if m.Attributes["Baz"] != string(m.Data) {
			t.Errorf("attribute 'Baz' incorrect attr: `%s`; data: `%s`", m.Attributes["Baz"], m.Data)
		}
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestPublish_withLogs(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	var count int
	nextFn := func() (pubsub.Message, error) {
		i := count
		count++
		if i >= n {
			return pubsub.Message{}, io.EOF
		}

		time.Sleep(100 * time.Microsecond)
		return pubsub.Message{
			Data: []byte(fmt.Sprintf("m_%06d\n", i)),
		}, nil
	}

	var logBuffer bytes.Buffer
	err := pkg.Publish(ctx, client, topic.ID(), nextFn, pkg.WithPublishStatsLogging(&logBuffer, 50*time.Millisecond))
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}

	logs := logBuffer.String()
	t.Log(logs)
	progressRegex := regexp.MustCompile("Progress \\(Total: [0-9]+; Success: [0-9]+\\)")
	completeRegex := regexp.MustCompile("Complete \\(Total: 10000; Success: 10000\\)")
	if matchCount := len(progressRegex.FindAllString(logs, -1)); matchCount < 1 {
		t.Errorf("wrong number of progress logs: %d", matchCount)
	}
	if matchCount := len(completeRegex.FindAllString(logs, -1)); matchCount != 1 {
		t.Errorf("wrong number of complete logs: %d", matchCount)
	}
}

func TestReceive_rateLimit(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	var count int
	nextFn := func() (pubsub.Message, error) {
		i := count
		count++
		if i >= n {
			return pubsub.Message{}, io.EOF
		}

		return pubsub.Message{
			Data: []byte(fmt.Sprintf("m_%06d\n", i)),
		}, nil
	}
	err := pkg.Publish(ctx, client, topic.ID(), nextFn)
	if err != nil {
		t.Error(err)
	}

	start := time.Now()
	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second), pkg.WithRateLimiter(rate.NewLimiter(rate.Limit(n)/7, 1)))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}
	duration := time.Since(start)
	if duration < 7*time.Second {
		t.Errorf("completed too quickly: %v", duration)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestReceive_rateLimitExternallyControlled(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	var count int
	nextFn := func() (pubsub.Message, error) {
		i := count
		count++
		if i >= n {
			return pubsub.Message{}, io.EOF
		}

		return pubsub.Message{
			Data: []byte(fmt.Sprintf("m_%06d\n", i)),
		}, nil
	}
	err := pkg.Publish(ctx, client, topic.ID(), nextFn)
	if err != nil {
		t.Error(err)
	}

	rl := rate.NewLimiter(0, 0)
	go func() {
		time.Sleep(3 * time.Second)
		rl.SetLimit(rate.Limit(n))
		rl.SetBurst(1)
	}()

	start := time.Now()
	got := make(map[string]int, n)
	err = pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(time.Second), pkg.WithRateLimiter(rl))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}
	duration := time.Since(start)
	if duration < 3*time.Second || duration > 6*time.Second {
		t.Errorf("completed in an unexpected duration: %v", duration)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestRepublish(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	t.Run("initial publish", func(t *testing.T) {
		initialTopic, initialSubscription := setupPubSub(t, ctx, client)
		defer topic.Stop()

		var count int
		nextFn := func() (pubsub.Message, error) {
			i := count
			count++
			if i >= n {
				return pubsub.Message{}, io.EOF
			}

			return pubsub.Message{
				Data: []byte(fmt.Sprintf("m_%06d\n", i)),
			}, nil
		}
		err := pkg.Publish(ctx, client, initialTopic.ID(), nextFn)
		if err != nil {
			t.Error(err)
		}

		err = pkg.Republish(ctx, client, topic.ID(), initialSubscription.ID(), 2*n)
		if err != nil && !errors.Is(err, pkg.ErrTimeout) {
			t.Error(err)
		}
	})

	got := make(map[string]int, n)
	err := pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(3*time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func TestRepublishLimit(t *testing.T) {
	const n int = 10000
	ctx, client, topic, subscription := setup(t)

	t.Run("initial publish", func(t *testing.T) {
		initialTopic, initialSubscription := setupPubSub(t, ctx, client)
		defer topic.Stop()

		var count int
		nextFn := func() (pubsub.Message, error) {
			i := count
			count++
			if i >= 2*n {
				return pubsub.Message{}, io.EOF
			}

			return pubsub.Message{
				Data: []byte(fmt.Sprintf("m_%06d\n", i)),
			}, nil
		}
		err := pkg.Publish(ctx, client, initialTopic.ID(), nextFn)
		if err != nil {
			t.Error(err)
		}

		err = pkg.Republish(ctx, client, topic.ID(), initialSubscription.ID(), n)
		if err != nil && !errors.Is(err, pkg.ErrMaxMessages) {
			t.Error(err)
		}
	})

	got := make(map[string]int, n)
	err := pkg.Receive(ctx, client, subscription.ID(), func(c context.Context, m *pubsub.Message) {
		got[string(m.Data)]++
		m.Ack()
	}, pkg.WithTimeout(3*time.Second))
	if err != nil && !errors.Is(err, pkg.ErrTimeout) {
		t.Error(err)
	}

	if len(got) != n {
		t.Errorf("wrong message count. want: %v; got: %v", n, len(got))
	}
}

func setup(t *testing.T) (context.Context, *pubsub.Client, *pubsub.Topic, *pubsub.Subscription) {
	t.Setenv("PUBSUB_EMULATOR_HOST", ":8089")
	t.Setenv("PUBSUB_PROJECT_ID", projectID)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConnectionPool(1))
	if err != nil {
		t.Error(err)
	}
	t.Cleanup(func() { client.Close() })

	subscriptions := client.Subscriptions(ctx)
	for {
		subscription, err := subscriptions.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Error(err)
		}
		err = subscription.Delete(ctx)
		if err != nil {
			t.Error(err)
		}
	}

	topics := client.Topics(ctx)
	for {
		topic, err := topics.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Error(err)
		}
		err = topic.Delete(ctx)
		if err != nil {
			t.Error(err)
		}
	}

	topic, subscription := setupPubSub(t, ctx, client)

	return ctx, client, topic, subscription
}

func setupPubSub(t *testing.T, ctx context.Context, client *pubsub.Client) (*pubsub.Topic, *pubsub.Subscription) {
	suffix := fmt.Sprintf("%d", rand.Int())
	topic := createTopic(t, ctx, client, "topic"+suffix)
	t.Cleanup(topic.Stop)
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
