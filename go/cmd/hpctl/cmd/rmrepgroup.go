package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

// keep arg here
var rmrgname string

var rmrgCmd = &cobra.Command{
	Use:   "rmrepgroup",
	Run:   rmRepGroup,
	Short: "Remove replication group from the cluster.",
	Long:  "Remove replication group from the cluster. You should rarely need this command. Replication group must not hold any partitions",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(rmrgCmd)

	rmrgCmd.Flags().StringVar(&rmrgname, "stolon-name", "",
		"cluster-name of Stolon instance being added. Must be unique for the whole hodgepodge cluster")
}

func rmRepGroup(cmd *cobra.Command, args []string) {
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
	}
	rg_found := false
	var rmrgid int
	for rgid, rg := range rgs {
		if rg.StolonName == rmrgname {
			rg_found = true
			rmrgid = rgid
		}
	}
	if !rg_found {
		die("There is no replication group named %s in the cluster", rmrgname)
	}

	tables, _, err := cs.GetTables(context.TODO())
	if err != nil {
		die("Failed to get tables from the store: %v", err)
	}
	for _, table := range tables {
		for pnum := 0; pnum < table.Nparts; pnum++ {
			if table.Partmap[pnum] == rmrgid {
				die("Cluster to be removed holds partition %d of table %s",
					pnum, table.Relname)
			}
		}
	}

	delete(rgs, rmrgid) // rmrg might be unaccessible, don't touch it
	err = cs.PutRepGroups(context.TODO(), rgs)
	if err != nil {
		die("failed to save repgroup data in store: %v", err)
	}
	stdout("Successfully deleted repgroup %s from the store", rmrgname)

	// try purge foreign servers from the rest of rgs
	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	for rgid, _ := range rgs {
		bcst.Push(rgid, fmt.Sprintf("drop server if exists hp_rg_%d cascade", rmrgid))
	}
	_, err = bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}
}
