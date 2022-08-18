package cmd

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"subtract/pkg"
)

var republishCmd = &cobra.Command{
	Use:   "republish",
	Short: "Republish messages from a subscription to a topic",
	Long: `Reads messages from a subscription and publishes them to a topic.
For example:

subtract republish \
 --project=my-project-id \
 --subscription=my-deadletter-subscription-name \
 --topic=my-retry-topic-name \
 --max=33`,
	Run: republish,
}

func init() {
	rootCmd.AddCommand(pullCmd)

	republishCmd.Flags().StringVar(&gcpProject, "project", "", "Name of the Google Cloud project")
	viper.BindPFlag("project", republishCmd.Flags().Lookup("project"))

	republishCmd.Flags().StringVar(&pubsubSubscription, "subscription", "", "PubSub subscription you wish to republish from")
	viper.BindPFlag("subscription", republishCmd.Flags().Lookup("subscription"))
	republishCmd.MarkFlagRequired("subscription")

	republishCmd.Flags().IntVar(&maxMessages, "max", 1, "The number of messages to pull")
	viper.BindPFlag("max", republishCmd.Flags().Lookup("max"))
}

func republish(cmd *cobra.Command, args []string) {
	client, err := pubsub.NewClient(cmd.Context(), gcpProject)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}

	topic := client.Topic(pubsubTopic)
	defer topic.Stop()

	err = pkg.ReceiveN(cmd.Context(), client, pubsubSubscription, maxMessages, func(c context.Context, m *pubsub.Message) {
		defer m.Nack()

		if verbose {
			cmd.Println("received message", m.ID)
		}

		_, err := topic.Publish(c, m).Get(c)
		if err != nil {
			m.Nack()
			cmd.PrintErrln(err)
			return
		}

		if verbose {
			fmt.Printf("published message %s. (Received: %i; Published: %i)", m.ID)
		}

		m.Ack()
	})
	if err != nil {
		cmd.PrintErrln(err)
	}
}
