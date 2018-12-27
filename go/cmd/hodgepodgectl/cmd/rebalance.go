// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/cluster/commands"
	"postgrespro.ru/hodgepodge/internal/pg"
)

var parallelism int

var rebCmd = &cobra.Command{
	Use:   "rebalance",
	Run:   rebalance,
	Short: "Rebalance the data: moves partitions between replication groups until they are evenly distributed. Based on logical replication and performed mostly seamlessly in background; each partition will be only shortly locked in the end to finally sync the data.",
	PreRun: func(c *cobra.Command, args []string) {
		if parallelism == 0 || parallelism < -1 {
			hl.Fatalf("Wrong parallelism")
		}
	},
}

func init() {
	rootCmd.AddCommand(rebCmd)

	rebCmd.Flags().IntVarP(&parallelism, "parallelism", "p", 10, "How many partitions to move simultaneously. Moving partitions one-by-one (1) minimizes overhead on cluster operation; -1 means maximum parallelism, all parts are moved at the same time.")
}

func rebalance(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	ctx := context.Background()
	tables, err := pg.GetTables(cs, ctx)
	if err != nil {
		hl.Fatalf("Failed to get tables from the store: %v", err)
	}

	rgs, _, err := cs.GetRepGroups(ctx)
	if err != nil {
		hl.Fatalf("Failed to get repgroups: %v", err)
	}

	var tasks = even_rebalance(tables, rgs)
	if err = commands.Rebalance(ctx, hl, cs, parallelism, tasks); err != nil {
		hl.Fatalf("%v", err)
	}
}

// form slice of tasks giving even rebalance
func even_rebalance(tables []cluster.Table, rgs map[int]*cluster.RepGroup) []commands.MoveTask {
	var tasks = make([]commands.MoveTask, 0)
	for _, table := range tables {
		if table.ColocateWithRelname != "" {
			continue // colocated tables follow their references
		}
		var parts_per_rg = make(map[int][]int)
		for rgid, _ := range rgs {
			parts_per_rg[rgid] = make([]int, 0)
		}
		// initial distribution
		for pnum := 0; pnum < table.Nparts; pnum++ {
			rgid := table.Partmap[pnum]
			if parts, ok := parts_per_rg[rgid]; ok {
				parts_per_rg[rgid] = append(parts, pnum)
			} else {
				hl.Fatalf("Metadata is broken: partholder %d is non-existing rgid", rgid)
			}
		}

		for { // rebalance until ideal
			var leanest_rgid = -1
			var leanest_rgid_parts int
			var fattest_rgid = -1
			var fattest_rgid_parts int
			for rgid, parts := range parts_per_rg {
				if leanest_rgid == -1 || len(parts) < leanest_rgid_parts {
					leanest_rgid, leanest_rgid_parts = rgid, len(parts)
				}
				if fattest_rgid == -1 || len(parts) > fattest_rgid_parts {
					fattest_rgid, fattest_rgid_parts = rgid, len(parts)
				}
			}
			if fattest_rgid_parts-leanest_rgid_parts <= 1 {
				break // done
			}
			// move part
			leanest_parts, fattest_parts := parts_per_rg[leanest_rgid], parts_per_rg[fattest_rgid]
			moved_part := fattest_parts[len(fattest_parts)-1]
			tasks = append(tasks, commands.MoveTask{
				SrcRgid:   fattest_rgid,
				DstRgid:   leanest_rgid,
				Schema:    table.Schema,
				TableName: table.Relname,
				Pnum:      moved_part,
			})
			hl.Debugf("Planned moving pnum %d for table %s from rg %d to rg %d\n",
				moved_part, table.Relname, fattest_rgid, leanest_rgid)
			parts_per_rg[leanest_rgid] = append(leanest_parts, moved_part)
			parts_per_rg[fattest_rgid] = fattest_parts[:len(fattest_parts)-1] // pop
			// move part of colocated tables
			for _, ctable := range tables {
				if ctable.ColocateWithSchema == table.Schema &&
					ctable.ColocateWithRelname == table.Relname {
					tasks = append(tasks, commands.MoveTask{
						SrcRgid:   fattest_rgid,
						DstRgid:   leanest_rgid,
						Schema:    ctable.Schema,
						TableName: ctable.Relname,
						Pnum:      moved_part,
					})
				}
			}
		}
	}
	return tasks
}
