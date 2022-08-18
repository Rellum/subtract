package cmd

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
)

// decodeCmd represents the decode command
var decodeCmd = &cobra.Command{
	Use:   "decode",
	Short: "Extracts and decodes the payload of a given Google PubSub json message",
	Long: `Reads JSON from stdin. For example:

echo "{\"Data\":\"ZXhhbWxlIHB1YnN1YiBtZXNzYWdlIGJvZHk\"}" | subtract decode`,
	Run: decode,
}

func init() {
	rootCmd.AddCommand(decodeCmd)
}

func decode(cmd *cobra.Command, args []string) {
	var msg pubSubMessage

	scanner := bufio.NewScanner(cmd.InOrStdin())
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &msg)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		payload, err := base64.StdEncoding.DecodeString(msg.Data)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(payload))
	}
	if err := scanner.Err(); err != nil {
		cmd.PrintErrln(err)
		return
	}
}
