package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
)

// we will store args directly in Table struct
var newtable cluster.Table

var hsCmd = &cobra.Command{
	Use:   "hash-shard-table",
	Run:   hashShardTable,
	Short: "Hash shard table.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if newtable.Relname == "" {
			die("relname is required")
		}
		if newtable.Sql == "" {
			die("sql is required")
		}
		if newtable.Nparts == 0 && newtable.ColocateWith == "" {
			die("Num of parts must be positive")
		}
	},
}

func init() {
	rootCmd.AddCommand(hsCmd)

	hsCmd.Flags().StringVar(&newtable.Relname, "relname", "", "Table name. No quoting is needed.")
	hsCmd.Flags().StringVar(&newtable.Sql, "sql",
		"", "SQL to create table, including 'partiton by hash' clause. For example, 'create table horns (id int primary key, branchness int) partition by hash (id)'")
	hsCmd.Flags().IntVar(&newtable.Nparts, "numparts", 0, "Number of partitions. Not needed (and ignored) if --colocate-with is used.")
	hsCmd.Flags().StringVar(&newtable.ColocateWith, "colocate-with", "", "Colocate this table with given one")
}

func hashShardTable(cmd *cobra.Command, args []string) {
	cs, err := cmdcommon.NewClusterStore(&cfg)
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
	var colocated_table *cluster.Table = nil
	for _, table := range tables {
		if table.Relname == newtable.Relname {
			die("Table %s is already sharded", newtable.Relname)
		}
		if newtable.ColocateWith == table.Relname {
			// if we are asked to colocate with already colocated table,
			// set its reference to avoid nested hierarchies
			if table.ColocateWith != "" {
				newtable.ColocateWith = table.ColocateWith
			}
			// We can use this reference though since partmap must
			// be the same
			colocated_table = &table
			newtable.Nparts = table.Nparts
		}
	}
	if newtable.ColocateWith != "" && colocated_table == nil {
		die("Table %s to colocate with is not found", newtable.ColocateWith)
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
	// create table everywhere
	for rgid, _ := range rgs {
		bcst.Push(rgid, fmt.Sprintf("drop table if exists %s cascade",
			pg.QI(newtable.Relname)))
		bcst.Push(rgid, newtable.Sql)
	}

	// extract rgids
	rgids := make([]int, 0, len(rgs))
	for rgid, _ := range rgs {
		rgids = append(rgids, rgid)
	}

	// create partitions
	var target_rgid int
	var target_rgid_idx int = -1
	newtable.Partmap = make([]int, newtable.Nparts)
	for pnum := 0; pnum < newtable.Nparts; pnum++ {
		// choose by plain round-robing, if not colocated
		if newtable.ColocateWith == "" {
			target_rgid_idx = (target_rgid_idx + 1) % len(rgids)
			target_rgid = rgids[target_rgid_idx]
		} else {
			target_rgid = colocated_table.Partmap[pnum]
		}
		newtable.Partmap[pnum] = target_rgid
		for rgid, _ := range rgs {
			if rgid == target_rgid { // holder
				bcst.Push(rgid, fmt.Sprintf("create table %s partition of %s for values with (modulus %d, remainder %d)",
					pg.QI(fmt.Sprintf("%s_%d", newtable.Relname, pnum)),
					pg.QI(newtable.Relname),
					newtable.Nparts,
					pnum))
			} else {
				bcst.Push(rgid, fmt.Sprintf("create foreign table %s partition of %s for values with (modulus %d, remainder %d) server hp_rg_%d options (table_name %s)",
					pg.QI(fmt.Sprintf("%s_%d_fdw", newtable.Relname, pnum)),
					pg.QI(newtable.Relname),
					newtable.Nparts,
					pnum,
					target_rgid,
					pg.QL(fmt.Sprintf("%s_%d", newtable.Relname, pnum))))
			}
		}
	}

	_, err = bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}
	tables = append(tables, newtable)
	err = cs.PutTables(context.TODO(), tables)
	if err != nil {
		die("failed to save tables data in store: %v", err)
	}
}
