// Copyright (c) 2018, Postgres Professional

package commands

import (
	"context"
	"fmt"

	"github.com/jackc/pgx"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/hplog"
	"postgrespro.ru/hodgepodge/internal/pg"
)

func RmRepGroup(ctx context.Context, hl *hplog.Logger, cs *cluster.ClusterStore, rmRepGroupName string) error {
	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get cluster data: %v", err)
	}
	if cldata == nil {
		return fmt.Errorf("data for cluster %v not found", cs.ClusterName)
	}

	rgs, _, err := cs.GetRepGroups(ctx)
	if err != nil {
		return fmt.Errorf("Failed to get repgroups: %v", err)
	}
	rg_found := false
	var rmrgid int
	for rgid, rg := range rgs {
		if rg.StolonName == rmRepGroupName {
			rg_found = true
			rmrgid = rgid
		}
	}
	if !rg_found {
		return fmt.Errorf("There is no replication group named %s in the cluster", rmRepGroupName)
	}

	tables, err := pg.GetTables(cs, ctx)
	if err != nil {
		return fmt.Errorf("Failed to retrieve tables: %v", err)
	}
	if len(tables) != 0 && len(rgs) <= 1 {
		return fmt.Errorf("Can't remove the last replication group with existing sharded tables")
	}
	var movetasks = free_rg_tasks(hl, rmrgid, tables, rgs)
	if len(movetasks) != 0 {
		hl.Infof("Replication group being removed holds partitions; moving them")
	}
	if err = Rebalance(ctx, hl, cs, 1, movetasks); err != nil {
		return fmt.Errorf("Failed to move tasks from removed repgroup: %v", err)
	}

	delete(rgs, rmrgid) // rmrg might be unaccessible, don't touch it
	// if there is someone left, purge removed rg from them
	for _, rg := range rgs {
		connstr, err := pg.GetSuConnstr(ctx, cs, rg, cldata)
		if err != nil {
			return fmt.Errorf("failed to get connstr of one of left rgs: %v", err)
		}
		connconfig, err := pgx.ParseConnectionString(connstr)
		conn, err := pgx.Connect(connconfig)
		if err != nil {
			return fmt.Errorf("Unable to connect to database: %v", err)
		}
		defer conn.Close()

		_, err = conn.Exec(fmt.Sprintf("select hodgepodge.rmrepgroup(%d)", rmrgid))
		if err != nil {
			return fmt.Errorf("failed to remove rg from others: %v", err)
		}
		break // broadcasted by hodgepodge.rmrepgroup
	}

	err = cs.PutRepGroups(ctx, rgs)
	if err != nil {
		return fmt.Errorf("failed to save repgroup data in store: %v\n  Note that rg was already removed from other rgs, so removal needs to be retried", err)
	}

	hl.Infof("Successfully deleted repgroup %s from the store", rmRepGroupName)
	return nil
}

// build tasks to move all partitions from given repgroup
func free_rg_tasks(hl *hplog.Logger, frgid int, tables []cluster.Table, rgs map[int]*cluster.RepGroup) []MoveTask {
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
					SrcRgid:   frgid,
					DstRgid:   rgids[dst_rg_idx],
					Schema:    table.Schema,
					TableName: table.Relname,
					Pnum:      pnum,
				})
				dst_rg_idx = (dst_rg_idx + 1) % len(rgids)
				hl.Debugf("Moving part %v of table %v to %v", pnum, table.Relname, rgids[dst_rg_idx])
			}
			for _, ctable := range tables {
				if ctable.ColocateWithSchema == table.Relname &&
					ctable.ColocateWithRelname == table.Relname {
					tasks = append(tasks, MoveTask{
						SrcRgid:   ctable.Partmap[pnum],
						DstRgid:   rgids[dst_rg_idx],
						Schema:    ctable.Schema,
						TableName: ctable.Relname,
						Pnum:      pnum,
					})
					dst_rg_idx = (dst_rg_idx + 1) % len(rgids)
				}
			}

		}
	}
	return tasks
}
