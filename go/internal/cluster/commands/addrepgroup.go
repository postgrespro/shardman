// Copyright (c) 2018, Postgres Professional

package commands

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/pg"
	"postgrespro.ru/shardman/internal/shmnlog"
)

func AddRepGroup(ctx context.Context, hl *shmnlog.Logger, cs *cluster.ClusterStore, hpc *cluster.StoreConnInfo, newrg *cluster.RepGroup) error {
	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get cluster data: %v", err)
	}
	if cldata == nil {
		return fmt.Errorf("cluster data not found in the store")
	}

	newconnstr, err := pg.GetSuConnstr(context.TODO(), cs, newrg, cldata)
	if err != nil {
		return fmt.Errorf("Couldn't get connstr: %v", err)
	}

	newconnconfig, err := pgx.ParseConnectionString(newconnstr)
	if err != nil {
		return fmt.Errorf("connstring parsing \"%s\" failed: %v", newconnstr, err) // should not happen
	}
	conn, err := pgx.Connect(newconnconfig)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %v", err)
	}
	defer conn.Close()

	// actually makes sense only for the first rg: dump/restore will recreate it anyway
	_, err = conn.Exec("drop extension if exists shardman cascade")
	if err != nil {
		return fmt.Errorf("Unable to drop ext: %v", err)
	}
	_, err = conn.Exec("create extension shardman cascade")
	if err != nil {
		return fmt.Errorf("Unable to create extension: %v", err)
	}
	err = conn.QueryRow("select system_identifier from pg_control_system()").Scan(&newrg.SysId)
	if err != nil {
		return fmt.Errorf("Failed to retrieve sysid: %v", err)
	}
	conn.Close() // safe to close multiple times

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		return fmt.Errorf("Failed to get repgroups: %v", err)
	}
	var newrgid int = 0
	for rgid, rg := range rgs {
		if rg.SysId == newrg.SysId {
			return fmt.Errorf("Repgroup with sys id %v already exists", rg.SysId)
		}
		if rgid > newrgid {
			newrgid = rgid
		}
	}
	newrgid++

	// stamp rgid in config
	err = cluster.StolonUpdate(hpc, newrg, newrgid, true, &cldata.Spec.StolonSpec)
	if err != nil {
		return err
	}
	// We just enabled prepared xacts and going to use them in broadcast;
	// wait until change is actually applied
	var max_attemtps = 3
	var attempt = 1
	for {
		hl.Infof("Waiting for config apply...")
		var max_prepared_transactions int
		update_conf_conn, err := pgx.Connect(newconnconfig)
		if err != nil {
			// system is shutting down
			if strings.Contains(err.Error(), "SQLSTATE 57P03") {
				time.Sleep(1 * time.Second)
				continue
			}
			if attempt == max_attemtps {
				return fmt.Errorf("Unable to connect to database: %v", err)
			}
			attempt++
			time.Sleep(1 * time.Second)
			continue
		}
		err = update_conf_conn.QueryRow("select setting::int from pg_settings where name='max_prepared_transactions'").Scan(&max_prepared_transactions)
		if err != nil {
			update_conf_conn.Close()
			return fmt.Errorf("Failed to check max_prepared_transactions: %v", err)
		}
		if max_prepared_transactions == 0 {
			time.Sleep(1 * time.Second)
		} else {
			hl.Infof("Done")
			update_conf_conn.Close()
			break
		}
		update_conf_conn.Close()
	}

	// do the terrible: pg_dump from random existing rg (if any), restore to new
	for _, rg := range rgs {
		existing_connstr, err := pg.GetSuConnstr(context.TODO(), cs, rg, cldata)
		if err != nil {
			return fmt.Errorf("failed to get connstr: %v", err)
		}
		dump_cmd := exec.Command("pg_dumpall", "--dbname", existing_connstr, "--clean", "--schema-only", "--if-exists")
		dump, err := dump_cmd.Output()
		if err != nil {
			var stderr = "<unknown>"
			switch e := err.(type) {
			case *exec.ExitError:
				stderr = string(e.Stderr[:])
			}
			return fmt.Errorf("pg_dumpall failed: %v, stderr: %v", err, stderr)
		}
		// pg_dump won't copy data from extension tables, do that instead of him...
		rgs_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy shardman.repgroups to stdout")
		rgs_dump, err := rgs_dump_cmd.Output()
		if err != nil {
			return fmt.Errorf("pg_dump failed: %v", err)
		}
		tables_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy shardman.sharded_tables to stdout")
		tables_dump, err := tables_dump_cmd.Output()
		if err != nil {
			return fmt.Errorf("pg_dump failed: %v", err)
		}
		parts_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy shardman.parts to stdout")
		parts_dump, err := parts_dump_cmd.Output()
		if err != nil {
			return fmt.Errorf("pg_dump failed: %v", err)
		}

		restore_cmd := exec.Command("psql", "--dbname", newconnstr)
		restore_cmd.Stdin = bytes.NewReader(dump)
		out, err := restore_cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		rgs_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy shardman.repgroups from stdin")
		rgs_restore_cmd.Stdin = bytes.NewReader(rgs_dump)
		out, err = rgs_restore_cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		tables_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy shardman.sharded_tables from stdin")
		tables_restore_cmd.Stdin = bytes.NewReader(tables_dump)
		out, err = tables_restore_cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		parts_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy shardman.parts from stdin")
		parts_restore_cmd.Stdin = bytes.NewReader(parts_dump)
		out, err = parts_restore_cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}

		break
	}

	rgs[newrgid] = newrg
	bcst, err := pg.NewBroadcaster(cs, rgs, cldata)
	if err != nil {
		return fmt.Errorf("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	// forbid all DDL during rg addition
	bcst.PushAll("lock shardman.repgroups in access exclusive mode")
	// create foreign servers to all rgs at newrg and vice versa
	newrgconnstrmap, err := cs.GetSuConnstrMap(context.TODO(), newrg, cldata)
	if err != nil {
		return fmt.Errorf("Failed to get new rg connstr")
	}
	newrgumopts, _ := pg.FormUserMappingOpts(newrgconnstrmap)
	newrgfsopts, _ := pg.FormForeignServerOpts(newrgconnstrmap)
	// insert myself
	bcst.Push(newrgid, fmt.Sprintf("insert into shardman.repgroups values (%d, null)", newrgid))
	// restamp oids
	bcst.Push(newrgid, "select shardman.restamp_oids()")
	for rgid, rg := range rgs {
		if rgid == newrgid {
			continue
		}
		rgconnstrmap, err := cs.GetSuConnstrMap(context.TODO(), rg, cldata)
		if err != nil {
			return fmt.Errorf("Failed to get rg %s connstr", rg.StolonName)
		}
		rgumopts, _ := pg.FormUserMappingOpts(rgconnstrmap)
		rgfsopts, _ := pg.FormForeignServerOpts(rgconnstrmap)
		bcst.Push(newrgid, fmt.Sprintf("drop server if exists %s cascade", pg.FSI(rgid)))
		bcst.Push(newrgid, fmt.Sprintf("create server %s foreign data wrapper shardman_postgres_fdw %s", pg.FSI(rgid), rgfsopts))
		bcst.Push(newrgid, fmt.Sprintf("update shardman.repgroups set srvid = (select oid from pg_foreign_server where srvname = %s) where id = %d",
			pg.FSL(rgid), rgid))
		bcst.Push(rgid, fmt.Sprintf("drop server if exists %s cascade", pg.FSI(newrgid)))
		bcst.Push(rgid, fmt.Sprintf("create server %s foreign data wrapper shardman_postgres_fdw %s", pg.FSI(newrgid), newrgfsopts))
		bcst.Push(rgid, fmt.Sprintf("insert into shardman.repgroups values (%d, (select oid from pg_foreign_server where srvname = %s))",
			newrgid, pg.FSL(newrgid)))
		// create sudo user mappings
		bcst.Push(newrgid, fmt.Sprintf("drop user mapping if exists for current_user server %s", pg.FSI(rgid)))
		bcst.Push(newrgid, fmt.Sprintf("create user mapping for current_user server %s %s", pg.FSI(rgid), rgumopts))
		bcst.Push(rgid, fmt.Sprintf("drop user mapping if exists for current_user server %s", pg.FSI(newrgid)))
		bcst.Push(rgid, fmt.Sprintf("create user mapping for current_user server %s %s", pg.FSI(newrgid), newrgumopts))
	}
	bcst.Push(newrgid, "select shardman.restore_foreign_tables()")
	_, err = bcst.Commit(true)
	if err != nil {
		return fmt.Errorf("bcst failed: %v", err)
	}

	// TODO make atomic
	err = cs.PutRepGroups(context.TODO(), rgs)
	if err != nil {
		return fmt.Errorf("failed to save repgroup data in store: %v", err)
	}

	return nil
}
