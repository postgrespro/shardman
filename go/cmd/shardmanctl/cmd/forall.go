// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/pg"
)

// for args
var sql string
var twophase bool

var faCmd = &cobra.Command{
	Use:   "forall",
	Run:   forall,
	Short: "Execute piece of SQL on all replication groups",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if sql == "" {
			hl.Fatalf("sql is required")
		}
	},
}

func init() {
	rootCmd.AddCommand(faCmd)

	faCmd.Flags().StringVar(&sql, "sql", "", "SQL to execute.")
	faCmd.Flags().BoolVar(&twophase, "twophase", false, "Use 2PC; gid is 'shardman' everywhere")
}

func forall(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	cldata, _, err := cs.GetClusterData(context.TODO())
	if err != nil {
		hl.Fatalf("cannot get cluster data: %v", err)
	}
	if cldata == nil {
		hl.Fatalf("cluster %v not found", cfg.ClusterName)
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		hl.Fatalf("Failed to get repgroups: %v", err)
	} else if len(rgs) == 0 {
		hl.Fatalf("Please add at least one repgroup")
	}
	bcst, err := pg.NewBroadcaster(cs, rgs, cldata)
	if err != nil {
		hl.Fatalf("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	for rgid, _ := range rgs {
		bcst.Push(rgid, sql)
	}

	results, err := bcst.Commit(twophase)
	if err != nil {
		hl.Fatalf("bcst failed: %v", err)
	}

	for rgid, res := range results {
		fmt.Printf("Node %d says:\n%s\n", rgid, res)
	}
}
