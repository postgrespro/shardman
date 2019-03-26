// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/cluster/commands"
)

// keep arg here
var rmrgname string

var rmrgCmd = &cobra.Command{
	Use:   "rmrepgroup",
	Run:   rmRepGroup,
	Short: "Remove replication group from the cluster.",
	Long:  "Remove replication group from the cluster. You should rarely need this command. If replication group holds any partitions, they will be moved to other repgroups",
}

func init() {
	rootCmd.AddCommand(rmrgCmd)

	rmrgCmd.Flags().StringVar(&rmrgname, "stolon-name", "",
		"cluster-name of repgroup (Stolon instance) to remove.")
}

func rmRepGroup(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	err = commands.RmRepGroup(context.Background(), hl, cs, rmrgname)
	if err != nil {
		hl.Fatalf("rmrepgroup failed: %v", err)
	}
}
