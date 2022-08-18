package cmd

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"subtract/pkg"
	"time"
)

// pullCmd represents the pull command
var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Pull messages from a Google PubSub subscription",
	Long: `Pull messages from a Google PubSub subscription. For example:

subtract pull \
 --project=my-project-id \
 --subscription=my-subscription-name \
 --max=33

Example output (pretty printed, actual output is single line json messages, one per line):
{
  "ID": "12345678",
  "Data": "ZXhhbWxlIHB1YnN1YiBtZXNzYWdlIGJvZHk=",
  "Attributes": {
    "CloudPubSubDeadLetterSourceDeliveryCount": "5",
    "CloudPubSubDeadLetterSourceSubscription": "my-subscription-name",
    "CloudPubSubDeadLetterSourceSubscriptionProject": "my-project-id",
    "CloudPubSubDeadLetterSourceTopicPublishTime": "2022-08-12T15:56:29.732+00:00"
  },
  "PublishTime": "2022-08-12T16:18:50.721Z",
  "DeliveryAttempt": null,
  "OrderingKey": ""
}`,
	Run: pull,
}

func init() {
	rootCmd.AddCommand(pullCmd)

	pullCmd.Flags().StringVar(&gcpProject, "project", "", "Name of the Google Cloud project")
	viper.BindPFlag("project", pullCmd.Flags().Lookup("project"))
	pullCmd.MarkFlagRequired("project")

	pullCmd.Flags().StringVar(&pubsubSubscription, "subscription", "", "Name of the PubSub subscription")
	viper.BindPFlag("subscription", pullCmd.Flags().Lookup("subscription"))
	pullCmd.MarkFlagRequired("subscription")

	pullCmd.Flags().IntVar(&maxMessages, "max", 1, "The number of messages to pull")
	viper.BindPFlag("max", pullCmd.Flags().Lookup("max"))
}

func pull(cmd *cobra.Command, args []string) {
	encoder := json.NewEncoder(cmd.OutOrStdout())

	client, err := pubsub.NewClient(cmd.Context(), gcpProject)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}

	ch := logPullProgress(cmd, time.Second*5)

	err = pkg.ReceiveN(cmd.Context(), client, pubsubSubscription, maxMessages, func(c context.Context, m *pubsub.Message) {
		if verbose {
			cmd.Println("received message", m.ID)
		}
		encoder.Encode(m)
		ch <- struct{}{}

		m.Ack()
	})
	if err == cmd.Context().Err() {
		// no error
	} else if err != nil {
		cmd.PrintErrln(err)
	}
}

func logPullProgress(cmd *cobra.Command, d time.Duration) chan<- struct{} {
	ch := make(chan struct{})

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	go func() {
		var fetched int
		for {
			select {
			case _, more := <-ch:
				if !more {
					return
				}
				fetched++
			case <-ticker.C:
				if verbose {
					cmd.Printf("Progress (Total: %d)\n", fetched)
				}
			case <-cmd.Context().Done():
				cmd.Printf("Finished (Total: %d)\n", fetched)
				return
			}
		}
	}()

	return ch
}
