package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

// for args
var sql string
var twophase bool

var faCmd = &cobra.Command{
	Use:   "forall",
	Run:   forall,
	Short: "Execute piece of SQL on all replication groups",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
		if sql == "" {
			die("sql is required")
		}
	},
}

func init() {
	rootCmd.AddCommand(faCmd)

	faCmd.Flags().StringVar(&sql, "sql", "", "SQL to execute.")
	faCmd.Flags().BoolVar(&twophase, "twophase", false, "Use 2PC; gid is 'hodgepodge' everywhere")
}

func forall(cmd *cobra.Command, args []string) {
	cs, err := store.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()

	cldata, _, err := cs.GetClusterData(context.TODO())
	if err != nil {
		die("cannot get cluster data: %v", err)
	}
	if cldata == nil {
		die("cluster %v not found", cfg.ClusterName)
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("Failed to get repgroups: %v", err)
	} else if len(rgs) == 0 {
		die("Please add at least one repgroup")
	}
	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	for rgid, _ := range rgs {
		bcst.Push(rgid, sql)
	}

	results, err := bcst.Commit(twophase)
	if err != nil {
		die("bcst failed: %v", err)
	}

	for rgid, res := range results {
		fmt.Printf("Node %d says:\n%s\n", rgid, res)
	}
}
