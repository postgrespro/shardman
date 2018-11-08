package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

var dtrelname string

var dtCmd = &cobra.Command{
	Use:   "drop-table",
	Run:   dropTable,
	Short: "Drop sharded table",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
		if dtrelname == "" {
			die("relname is required")
		}
	},
}

func init() {
	rootCmd.AddCommand(dtCmd)

	dtCmd.Flags().StringVar(&dtrelname, "relname", "", "Table name. No quoting is needed.")
}

func dropTable(cmd *cobra.Command, args []string) {
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

	tables, _, err := cs.GetTables(context.TODO())
	if err != nil {
		die("Failed to get tables from the store: %v", err)
	}
	var kept_tables = make([]cluster.Table, 0, len(tables))
	var table_found = false
	for _, table := range tables {
		if table.Relname == dtrelname {
			table_found = true
			continue
		}
		// remove colocation
		if table.ColocateWith == dtrelname {
			table.ColocateWith = ""
		}
		kept_tables = append(kept_tables, table)
	}
	if !table_found {
		die("Table %s is not sharded", dtrelname)
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("Failed to get repgroups: %v", err)
	}

	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()
	bcst.Begin()

	for rgid, _ := range rgs {
		bcst.Push(rgid, fmt.Sprintf("drop table if exists %s cascade",
			pg.QI(dtrelname)))
	}

	_, err = bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}
	err = cs.PutTables(context.TODO(), kept_tables)
	if err != nil {
		die("failed to save tables data in store: %v", err)
	}
}
