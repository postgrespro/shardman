package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"postgrespro.ru/shardman/internal/ladle"
)

var fixRepGroupsCmd = &cobra.Command{
	Use:   "fixrepgroups",
	Run:   fixRepGroups,
	Short: "Fix repgroups after interrupted addnodes",
}

func init() {
	rootCmd.AddCommand(fixRepGroupsCmd)
}

func fixRepGroups(cmd *cobra.Command, args []string) {
	ls, err := ladle.NewLadleStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer ls.Close()

	err = ls.FixRepGroups(context.Background(), hl)
	if err != nil {
		hl.Fatalf("failed to fix repgroups: %v", err)
	}
}
