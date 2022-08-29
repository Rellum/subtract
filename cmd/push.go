package cmd

import (
	"cloud.google.com/go/pubsub"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"sync"
	"time"
)

// pushCmd represents the push command
var pushCmd = &cobra.Command{
	Use:   "push",
	Short: "Push messages to a Google PubSub topic",
	Long: `Push messages to a Google PubSub topic

The difference between 'publish' and 'push' is that the 'publish' reads message bodies (payloads) 
and 'push' reads PubSub message "envelopes" (which might have been fetched using 'pull').

For example:

subtract push \
 --project=my-project-id \
 --topic=my-topic-name
`,
	Run: push,
}

func init() {
	rootCmd.AddCommand(pushCmd)

	pushCmd.Flags().StringVar(&gcpProject, "project", "", "Name of the Google Cloud project")
	viper.BindPFlag("project", pushCmd.Flags().Lookup("project"))
	pushCmd.MarkFlagRequired("project")
}

func push(cmd *cobra.Command, args []string) {
	decoder := json.NewDecoder(cmd.InOrStdin())

	client, err := pubsub.NewClient(cmd.Context(), gcpProject)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}

	publishCh, errCh := logResults(cmd, 5*time.Second)
	defer func() { close(errCh) }()

	topic := client.Topic(pubsubTopic)
	defer topic.Stop()

	var wg sync.WaitGroup
	for {
		var m pubsub.Message
		if err = decoder.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			cmd.PrintErrln(err)
			return
		}

		res := topic.Publish(cmd.Context(), &m)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-res.Ready()
			_, err := res.Get(cmd.Context())
			if err != nil {
				errCh <- fmt.Errorf("message %s: %w", m.ID, err)
			} else {
				errCh <- nil
			}
		}()

		publishCh <- struct{}{}
	}

	wg.Wait()
}

func logResults(cmd *cobra.Command, d time.Duration) (chan<- struct{}, chan<- error) {
	publishCh := make(chan struct{})
	errorCh := make(chan error)

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	go func() {
		var (
			published int
			results   int
			errors    int
		)
		for {
			select {
			case <-publishCh:
				published++
			case receivedErr, more := <-errorCh:
				if !more {
					return
				}
				results++
				if receivedErr != nil {
					errors++
					cmd.PrintErrln(receivedErr)
				}
			case <-ticker.C:
				if verbose {
					cmd.Printf("Progress (Total: %d; Success: %d; Failures: %d)\n", published, results-errors, errors)
				}
			case <-cmd.Context().Done():
				cmd.Printf("Finished (Total: %d; Success: %d; Failures: %d)\n", published, results-errors, errors)
				return
			}
		}
	}()

	return publishCh, errorCh
}
