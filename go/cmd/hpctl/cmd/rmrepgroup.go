// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
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
		"cluster-name of Stolon instance being added. Must be unique for the whole hodgepodge cluster")
}

func rmRepGroup(cmd *cobra.Command, args []string) {
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
	if len(tables) != 0 && len(rgs) <= 1 {
		die("Can't the last replication group with existing sharded tables")
	}
	var movetasks = free_rg_tasks(rmrgid, tables, rgs)
	if len(movetasks) != 0 {
		stderr("Replication group being removed holds partitions; moving them")
	}
	if !Rebalance(cs, 1, movetasks) {
		die("Failed to move tasks from removed repgroup")
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

// build tasks to move all partitions from given repgroup
func free_rg_tasks(frgid int, tables []cluster.Table, rgs map[int]*cluster.RepGroup) []MoveTask {
	var tasks = make([]MoveTask, 0)
	var rgids = make([]int, len(rgs)-1)
	var i = 0
	for rgid, _ := range rgs {
		if rgid == frgid {
			continue
		}
		rgids[i] = rgid
		i++
	}
	var dst_rg_idx = 0
	for _, table := range tables {
		if table.ColocateWith != "" {
			continue // colocated tables follow their references
		}
		for pnum := 0; pnum < table.Nparts; pnum++ {
			if frgid == table.Partmap[pnum] {
				tasks = append(tasks, MoveTask{
					src_rgid:   frgid,
					dst_rgid:   rgids[dst_rg_idx],
					table_name: table.Relname,
					pnum:       pnum,
				})
			}
			for _, ctable := range tables {
				if ctable.ColocateWith == table.Relname {
					tasks = append(tasks, MoveTask{
						src_rgid:   ctable.Partmap[pnum],
						dst_rgid:   rgids[dst_rg_idx],
						table_name: ctable.Relname,
						pnum:       pnum,
					})
				}
			}

		}
		dst_rg_idx = (dst_rg_idx + 1) % len(rgids)
	}
	return tasks
}
