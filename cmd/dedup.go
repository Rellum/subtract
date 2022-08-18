package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
)

// dedupCmd represents the dedup command
var dedupCmd = &cobra.Command{
	Use:   "dedup",
	Short: "Swallows messages with the same payload as a previous message",
	Long: `Reads JSON from stdin. For example:

subtract pull ... | subtract dedup`,
	Run: dedup,
}

func init() {
	rootCmd.AddCommand(dedupCmd)
}

func dedup(cmd *cobra.Command, args []string) {
	payloads := make(map[string]struct{})
	var msg pubSubMessage

	scanner := bufio.NewScanner(cmd.InOrStdin())
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &msg)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		if _, ok := payloads[msg.Data]; ok {
			continue
		}

		payloads[msg.Data] = struct{}{}

		fmt.Fprintln(cmd.OutOrStdout(), scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		cmd.PrintErrln(err)
		return
	}
}
