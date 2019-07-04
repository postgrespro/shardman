// Copyright (c) 2019, Postgres Professional

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/pg"
)

// for args
var directMasters bool

var getConnstrCmd = &cobra.Command{
	Use:   "getconnstr",
	Run:   getConnstr,
	Short: "Get connection string of the cluster",
}

func init() {
	rootCmd.AddCommand(getConnstrCmd)

	getConnstrCmd.Flags().BoolVar(&directMasters, "direct-masters", false, "Retrieve direct masters addresses, not proxies even if UseProxy is true")
}

func getConnstr(cmd *cobra.Command, args []string) {
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

	// collect and glue connstrs from all rgs
	var connstrmap map[string]string = nil
	for rgid, rg := range rgs {
		rgConnstrmap, _, err := cs.GetSuConnstrMapExtended(context.TODO(), rg, cldata, directMasters, false)
		if err != nil {
			hl.Fatalf("Failed to get connstrmap of rgid %v: %v", rgid, err)
		}
		if connstrmap == nil {
			connstrmap = rgConnstrmap
		} else {
			connstrmap["host"] += "," + rgConnstrmap["host"]
			connstrmap["port"] += "," + rgConnstrmap["port"]
		}
	}
	connstr := pg.ConnString(connstrmap)
	// and print the result
	fmt.Printf(connstr + "\n")
}
