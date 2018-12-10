// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"

	"github.com/jackc/pgx"
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

	tables, err := pg.GetTables(cs, context.TODO())
	if err != nil {
		die("Failed to retrieve tables: %v", err)
	}
	if len(tables) != 0 && len(rgs) <= 1 {
		die("Can't remove the last replication group with existing sharded tables")
	}
	var movetasks = free_rg_tasks(rmrgid, tables, rgs)
	if len(movetasks) != 0 {
		stderr("Replication group being removed holds partitions; moving them")
	}
	if !Rebalance(cs, 1, movetasks) {
		die("Failed to move tasks from removed repgroup")
	}

	delete(rgs, rmrgid) // rmrg might be unaccessible, don't touch it
	// if there is someone left, purge removed rg from them
	for _, rg := range rgs {
		connstr, err := pg.GetSuConnstr(context.TODO(), rg, cldata)
		if err != nil {
			die("failed to get connstr of one of left rgs: %v", err)
		}
		connconfig, err := pgx.ParseConnectionString(connstr)
		conn, err := pgx.Connect(connconfig)
		if err != nil {
			die("Unable to connect to database: %v", err)
		}
		defer conn.Close()

		_, err = conn.Exec(fmt.Sprintf("select hodgepodge.rmrepgroup(%d)", rmrgid))
		if err != nil {
			die("failed to remove rg from others: %v", err)
		}
		break
	}

	err = cs.PutRepGroups(context.TODO(), rgs)
	if err != nil {
		die("failed to save repgroup data in store: %v\n  Note that rg was already removed from other rgs", err)
	}
	stdout("Successfully deleted repgroup %s from the store", rmrgname)
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
		if table.ColocateWithSchema != "" {
			continue // colocated tables follow their references
		}
		for pnum := 0; pnum < table.Nparts; pnum++ {
			if frgid == table.Partmap[pnum] {
				tasks = append(tasks, MoveTask{
					src_rgid:   frgid,
					dst_rgid:   rgids[dst_rg_idx],
					schema:     table.Schema,
					table_name: table.Relname,
					pnum:       pnum,
				})
			}
			for _, ctable := range tables {
				if ctable.ColocateWithSchema == table.Relname &&
					ctable.ColocateWithRelname == table.Relname {
					tasks = append(tasks, MoveTask{
						src_rgid:   ctable.Partmap[pnum],
						dst_rgid:   rgids[dst_rg_idx],
						schema:     ctable.Schema,
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
