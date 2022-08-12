package cmd

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var gcpProject string
var pubsubSubscription string
var max int

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

	pullCmd.Flags().IntVar(&max, "max", 1, "The number of messages to pull")
	viper.BindPFlag("max", pullCmd.Flags().Lookup("max"))
}

func pull(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	encoder := json.NewEncoder(cmd.OutOrStdout())

	counter := make(chan struct{})
	go func() {
		acceptN(max, counter)
		cancel()
	}()

	err = client.Subscription(pubsubSubscription).Receive(ctx, func(c context.Context, m *pubsub.Message) {
		defer m.Nack()

		select {
		case counter <- struct{}{}:
		case <-ctx.Done():
			return
		}

		if Verbose {
			cmd.Print("received message", m.ID)
		}
		encoder.Encode(m)

		m.Ack()
	})
	if err != nil {
		cmd.PrintErr(err)
	}
}

func acceptN(n int, ch <-chan struct{}) {
	for {
		n--
		if n < 0 {
			return
		}
		<-ch
	}
}
