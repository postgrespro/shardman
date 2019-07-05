// Copyright (c) 2019, Postgres Professional

package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/pg"
)

// for args
var directMasters bool

var getConnstrCmd = &cobra.Command{
	Use:   "getconnstr",
	Run:   getConnstr,
	Short: "Get connection string of the cluster. It reshuffles hosts on every call, which is useful because currently libpq always tries multiple endpoints sequentially, but for distributing workload clients should connect to different endpoints.",
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
	hosts := make([]string, 0)
	ports := make([]string, 0)
	for rgid, rg := range rgs {
		rgConnstrmap, _, err := cs.GetSuConnstrMapExtended(context.TODO(), rg, cldata, directMasters, false)
		if err != nil {
			hl.Fatalf("Failed to get connstrmap of rgid %v: %v", rgid, err)
		}
		if connstrmap == nil {
			connstrmap = rgConnstrmap
		}
		hosts = append(hosts, rgConnstrmap["host"])
		ports = append(ports, rgConnstrmap["port"])
	}

	// reshuffle hosts and ports
	// randomize seed
	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
		ports[i], ports[j] = ports[j], ports[i]
	})

	connstrmap["host"] = strings.Join(hosts, ",")
	connstrmap["port"] = strings.Join(ports, ",")

	connstr := pg.ConnString(connstrmap)
	// and print the result
	fmt.Printf(connstr + "\n")
}
