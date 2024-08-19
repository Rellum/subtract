package pkg

import (
	"fmt"
	"io"
	"time"
)

type Stats struct {
	IsComplete bool
	Total      int
	Errors     []error
}

func WithPublishStatsLogging(w io.Writer, interval time.Duration) func(*publishOptions) {
	return WithPublishStatsFunc(interval, logStats(w))
}

func WithPublishStatsFunc(interval time.Duration, statsFunc func(Stats)) func(*publishOptions) {
	return func(o *publishOptions) {
		o.statsInterval = interval
		o.statsFunc = statsFunc
	}
}

func WithReceiveStatsLogging(w io.Writer, interval time.Duration) func(options *receiveOptions) {
	return WithReceiveStatsFunc(interval, logStats(w))
}

func WithReceiveStatsFunc(interval time.Duration, statsFunc func(Stats)) func(options *receiveOptions) {
	return func(o *receiveOptions) {
		o.statsInterval = interval
		o.statsFunc = statsFunc
	}
}

func logStats(w io.Writer) func(stats Stats) {
	return func(stats Stats) {
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
