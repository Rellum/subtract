package pkg

import (
	"bufio"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

func Publish(ctx context.Context, client *pubsub.Client, pubsubTopic string, next func() (pubsub.Message, error), opts ...func(*publishOptions)) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	o := publishOptions{
		statsFunc:     func(stats) {},
		statsInterval: 5 * time.Second,
	}
	for i := range opts {
		opts[i](&o)
	}

	topic := client.Topic(pubsubTopic)
	defer topic.Stop()

	errCh := make(chan error)

	var mu sync.Mutex
	var total int
	var errs []error
	go func() {
		mu.Lock()
		defer mu.Unlock()
		for queuedErr := range errCh {
			if errors.Is(queuedErr, context.Canceled) {
				continue
			}
			total++
			if queuedErr == nil {
				continue
			}
			errs = append(errs, queuedErr)
			if !o.continueOnErrors {
				cancel()
			}
		}
	}()

	ticker := time.NewTicker(o.statsInterval)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				stats := stats{
					IsComplete: false,
					Total:      total,
					Errors:     append([]error{}, errs...),
				}
				mu.Unlock()
				o.statsFunc(stats)
			}
		}
	}()

	var wg sync.WaitGroup
	for {
		message, err := next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		wg.Add(1)
		res := topic.Publish(cctx, &message)

		go func() {
			defer wg.Done()
			id, err := res.Get(cctx)
			if err != nil {
				errCh <- fmt.Errorf("message %s: %w", id, err)
			} else {
				message.Ack()
				errCh <- nil
			}
		}()
	}
	wg.Wait()
	close(errCh)

	mu.Lock()
	stats := stats{
		IsComplete: true,
		Total:      total,
		Errors:     append([]error{}, errs...),
	}
	mu.Unlock()
	o.statsFunc(stats)

	if len(stats.Errors) > 0 {
		return stats.Errors[len(stats.Errors)-1]
	}
	return nil
}

func ScanPayloads(scanner *bufio.Scanner) func() (pubsub.Message, error) {
	return func() (pubsub.Message, error) {
		more := scanner.Scan()
		if !more {
			return pubsub.Message{}, io.EOF
		}

		if err := scanner.Err(); err != nil {
			return pubsub.Message{}, err
		}

		b := append([]byte{}, scanner.Bytes()...)
		return pubsub.Message{
			Data: b,
		}, nil
	}
}

func ScanMessages(scanner *bufio.Scanner) func() (pubsub.Message, error) {
	return func() (pubsub.Message, error) {
		more := scanner.Scan()
		if !more {
			return pubsub.Message{}, io.EOF
		}

		if err := scanner.Err(); err != nil {
			return pubsub.Message{}, err
		}

		var m pubsub.Message
		return m, json.Unmarshal(scanner.Bytes(), &m)
	}
}

type stats struct {
	IsComplete bool
	Total      int
	Errors     []error
}

type publishOptions struct {
	continueOnErrors bool
	statsInterval    time.Duration
	statsFunc        func(stats)
}

func WithContinueOnErrors() func(*publishOptions) {
	return func(o *publishOptions) {
		o.continueOnErrors = true
	}
}

func WithStatsLogging(w io.Writer, interval time.Duration) func(*publishOptions) {
	return withStatsFunc(interval, logStats(w))
}

func withStatsFunc(interval time.Duration, statsFunc func(stats)) func(*publishOptions) {
	return func(o *publishOptions) {
		o.statsInterval = interval
		o.statsFunc = statsFunc
	}
}

func logStats(w io.Writer) func(stats stats) {
	return func(stats stats) {
		errCount := len(stats.Errors)

		progressText := "Progress"
		if stats.IsComplete {
			progressText = "Complete"
		}

		var lastErrorText string
		if errCount > 0 {
			lastErrorText = fmt.Sprintf("; Last error: %v", stats.Errors[errCount-1])
		}
		fmt.Fprintf(w, "%s (Total: %d; Success: %d%s)\n", progressText, stats.Total, stats.Total-errCount, lastErrorText)
	}
}
