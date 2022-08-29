package cmd

import (
	"bufio"
	"bytes"
	"cloud.google.com/go/pubsub"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sync"
	"time"
)

// publishCmd represents the publish command
var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish message payloads to a Google PubSub topic",
	Long: `Publish message payloads to a Google PubSub topic.
The difference between 'publish' and 'push' is that the 'publish' reads message bodies (payloads) 
and 'push' reads PubSub message "envelopes" (which might have been fetched using 'pull').

For example:

subtract publish \
 --project=my-project-id \
 --topic=my-topic-name
`,
	Run: publish,
}

func init() {
	rootCmd.AddCommand(publishCmd)

	publishCmd.Flags().StringVar(&gcpProject, "project", "", "Name of the Google Cloud project")
	viper.BindPFlag("project", publishCmd.Flags().Lookup("project"))
	publishCmd.MarkFlagRequired("project")

	publishCmd.Flags().StringVar(&delimitString, "delimit", "\n", "String that delimits messages")
	viper.BindPFlag("delimit", publishCmd.Flags().Lookup("delimit"))
}

func publish(cmd *cobra.Command, args []string) {
	scanner := bufio.NewScanner(cmd.InOrStdin())
	scanner.Split(splitAt(delimitString))

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
	for scanner.Scan() {
		var m pubsub.Message
		m.Data = scanner.Bytes()

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
	if err := scanner.Err(); err != nil {
		cmd.PrintErrln(err)
	}

	wg.Wait()
}

func splitAt(substring string) func(data []byte, atEOF bool) (advance int, token []byte, err error) {
	searchBytes := []byte(substring)
	searchLen := len(searchBytes)
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		dataLen := len(data)

		// Return nothing if at end of file and no data passed
		if atEOF && dataLen == 0 {
			return 0, nil, nil
		}

		// Find next separator and return token
		if i := bytes.Index(data, searchBytes); i >= 0 {
			return i + searchLen, data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated string. Return it.
		if atEOF {
			return dataLen, data, nil
		}

		// Request more data.
		return 0, nil, nil
	}
}
