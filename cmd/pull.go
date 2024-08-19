package cmd

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
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

	pullCmd.Flags().IntVar(&maxMessages, "max", 0, "The number of messages to pull. Zero means unlimited.")
	viper.BindPFlag("max", pullCmd.Flags().Lookup("max"))

	pullCmd.Flags().Float64Var(&rateLimit, "rate", 0, "The number of messages to pull per second. Fractions are possible. Zero means no limit.")
	viper.BindPFlag("rate", pullCmd.Flags().Lookup("rate"))

	pullCmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "The period to wait before assuming the subscription is empty.")
	viper.BindPFlag("timeout", pullCmd.Flags().Lookup("timeout"))
}

func pull(cmd *cobra.Command, args []string) {
	encoder := json.NewEncoder(cmd.OutOrStdout())

	client, err := pubsub.NewClient(cmd.Context(), gcpProject)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}

	rl := rate.NewLimiter(rate.Limit(rateLimit), 1)
	if rateLimit == 0 {
		rl.SetLimit(rate.Inf)
	}
	opts := []pkg.ReceiveOption{
		pkg.WithRateLimiter(rl),
		pkg.WithTimeout(timeout),
		pkg.WithReceiveStatsLogging(cmd.ErrOrStderr(), 5*time.Second),
	}

	if maxMessages > 0 {
		opts = append(opts, pkg.WithMaxMessages(maxMessages))
	}

	err = pkg.Receive(cmd.Context(), client, pubsubSubscription, func(c context.Context, m *pubsub.Message) {
		if verbose {
			cmd.Println("received message", m.ID)
		}
		encoder.Encode(m)

		m.Ack()
	}, opts...)
	if err == cmd.Context().Err() {
		// no error
	} else if err != nil {
		cmd.PrintErrln(err)
	}
}
