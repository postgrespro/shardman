// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"

	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

// we will store args directly in RepGroup struct
var newrg cluster.RepGroup

var addrgCmd = &cobra.Command{
	Use:   "addrepgroup",
	Run:   addRepGroup,
	Short: "Add replication group to the cluster",
}

func init() {
	rootCmd.AddCommand(addrgCmd)

	addrgCmd.Flags().StringVar(&newrg.StolonName, "stolon-name", "",
		"cluster-name of Stolon instance being added. Must be unique for the whole hodgepodge cluster")
	addrgCmd.Flags().StringVar(&newrg.StoreEndpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0],
		"a comma-delimited list of Stolon store endpoints (use https scheme for tls communication)")
	addrgCmd.Flags().StringVar(&newrg.StoreCAFile, "store-ca-file", "",
		"verify certificates of HTTPS-enabled Stolon store servers using this CA bundle")
	addrgCmd.Flags().StringVar(&newrg.StoreCertFile, "store-cert-file", "",
		"certificate file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&newrg.StoreKey, "store-key", "",
		"private key file for client identification to the Stolon store")
	addrgCmd.Flags().BoolVar(&newrg.StoreSkipTLSVerify, "store-skip-tls-verify",
		false, "skip Stolon store certificate verification")
	addrgCmd.Flags().StringVar(&newrg.StorePrefix, "store-prefix", "stolon/cluster",
		"the Stolon store base prefix")
}

func addRepGroup(cmd *cobra.Command, args []string) {
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

	newconnstr, err := pg.GetSuConnstr(context.TODO(), &newrg, cldata)
	if err != nil {
		die("Couldn't get connstr: %v", err)
	}

	newconnconfig, err := pgx.ParseConnectionString(newconnstr)
	if err != nil {
		die("connstring parsing \"%s\" failed: %v", newconnstr, err) // should not happen
	}
	conn, err := pgx.Connect(newconnconfig)
	if err != nil {
		die("Unable to connect to database: %v", err)
	}
	defer conn.Close()

	// actually makes sense only for the first rg: dump/restore will recreate it anyway
	_, err = conn.Exec("drop extension if exists hodgepodge cascade")
	if err != nil {
		die("Unable to drop ext: %v", err)
	}
	_, err = conn.Exec("create extension hodgepodge cascade")
	if err != nil {
		die("Unable to create extension: %v", err)
	}
	err = conn.QueryRow("select system_identifier from pg_control_system()").Scan(&newrg.SysId)
	if err != nil {
		die("Failed to retrieve sysid: %v", err)
	}
	conn.Close() // safe to close multiple times

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("Failed to get repgroups: %v", err)
	}
	var newrgid int = 0
	for rgid, rg := range rgs {
		if rg.SysId == newrg.SysId {
			die("Repgroup with sys id %v already exists", rg.SysId)
		}
		if rgid > newrgid {
			newrgid = rgid
		}
	}
	newrgid++

	err = store.StolonUpdate(&newrg, newrgid, true, cldata.StolonSpec)
	if err != nil {
		die(err.Error())
	}
	// We just enabled prepared xacts and going to use them in broadcast;
	// wait until change is actually applied
	var max_attemtps = 3
	var attempt = 1
	for {
		stderr("Waiting for config apply...")
		var max_prepared_transactions int
		update_conf_conn, err := pgx.Connect(newconnconfig)
		if err != nil {
			// system is shutting down
			if strings.Contains(err.Error(), "SQLSTATE 57P03") {
				time.Sleep(1 * time.Second)
				continue
			}
			if attempt == max_attemtps {
				die("Unable to connect to database: %v", err)
			}
			attempt++
			time.Sleep(1 * time.Second)
			continue
		}
		err = update_conf_conn.QueryRow("select setting::int from pg_settings where name='max_prepared_transactions'").Scan(&max_prepared_transactions)
		if err != nil {
			update_conf_conn.Close()
			die("Failed to check max_prepared_transactions: %v", err)
		}
		if max_prepared_transactions == 0 {
			time.Sleep(1 * time.Second)
		} else {
			stderr("Done")
			update_conf_conn.Close()
			break
		}
		update_conf_conn.Close()
	}

	// do the terrible: pg_dump from random existing rg (if any), restore to new
	for _, rg := range rgs {
		existing_connstr, err := pg.GetSuConnstr(context.TODO(), rg, cldata)
		if err != nil {
			die("failed to get connstr: %v", err)
		}
		dump_cmd := exec.Command("pg_dumpall", "--dbname", existing_connstr, "--clean", "--schema-only", "--if-exists")
		dump, err := dump_cmd.Output()
		if err != nil {
			exiterr := err.(*exec.ExitError)
			die("pg_dumpall failed: %v, stderr: %v", exiterr, string(exiterr.Stderr[:]))
		}
		// pg_dump won't copy data from extension tables, do that instead of him...
		rgs_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy hodgepodge.repgroups to stdout")
		rgs_dump, err := rgs_dump_cmd.Output()
		if err != nil {
			die("pg_dump failed: %v", err)
		}
		tables_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy hodgepodge.sharded_tables to stdout")
		tables_dump, err := tables_dump_cmd.Output()
		if err != nil {
			die("pg_dump failed: %v", err)
		}
		parts_dump_cmd := exec.Command("psql", "--dbname", existing_connstr, "-c", "copy hodgepodge.parts to stdout")
		parts_dump, err := parts_dump_cmd.Output()
		if err != nil {
			die("pg_dump failed: %v", err)
		}

		restore_cmd := exec.Command("psql", "--dbname", newconnstr)
		restore_cmd.Stdin = bytes.NewReader(dump)
		out, err := restore_cmd.CombinedOutput()
		if err != nil {
			die("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		rgs_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy hodgepodge.repgroups from stdin")
		rgs_restore_cmd.Stdin = bytes.NewReader(rgs_dump)
		out, err = rgs_restore_cmd.CombinedOutput()
		if err != nil {
			die("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		tables_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy hodgepodge.sharded_tables from stdin")
		tables_restore_cmd.Stdin = bytes.NewReader(tables_dump)
		out, err = tables_restore_cmd.CombinedOutput()
		if err != nil {
			die("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}
		parts_restore_cmd := exec.Command("psql", "--dbname", newconnstr, "-c", "copy hodgepodge.parts from stdin")
		parts_restore_cmd.Stdin = bytes.NewReader(parts_dump)
		out, err = parts_restore_cmd.CombinedOutput()
		if err != nil {
			die("psql dump restore failed: %v; stderr/out is %v", err, string(out[:]))
		}

		break
	}

	rgs[newrgid] = &newrg
	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	// forbid all DDL during rg addition
	bcst.PushAll("lock hodgepodge.repgroups in access exclusive mode")
	// create foreign servers to all rgs at newrg and vice versa
	newrgconnstrmap, err := store.GetSuConnstrMap(context.TODO(), &newrg, cldata)
	if err != nil {
		die("Failed to get new rg connstr")
	}
	newrgumopts, _ := pg.FormUserMappingOpts(newrgconnstrmap)
	newrgfsopts, _ := pg.FormForeignServerOpts(newrgconnstrmap)
	// insert myself
	bcst.Push(newrgid, fmt.Sprintf("insert into hodgepodge.repgroups values (%d, null)", newrgid))
	// restamp oids
	bcst.Push(newrgid, "select hodgepodge.restamp_oids()")
	for rgid, rg := range rgs {
		if rgid == newrgid {
			continue
		}
		rgconnstrmap, err := store.GetSuConnstrMap(context.TODO(), rg, cldata)
		if err != nil {
			die("Failed to get rg %s connstr", rg.StolonName)
		}
		rgumopts, _ := pg.FormUserMappingOpts(rgconnstrmap)
		rgfsopts, _ := pg.FormForeignServerOpts(rgconnstrmap)
		bcst.Push(newrgid, fmt.Sprintf("drop server if exists %s cascade", pg.FSI(rgid)))
		bcst.Push(newrgid, fmt.Sprintf("create server %s foreign data wrapper hodgepodge_postgres_fdw %s", pg.FSI(rgid), rgfsopts))
		bcst.Push(newrgid, fmt.Sprintf("update hodgepodge.repgroups set srvid = (select oid from pg_foreign_server where srvname = %s) where id = %d",
			pg.FSL(rgid), rgid))
		bcst.Push(rgid, fmt.Sprintf("drop server if exists %s cascade", pg.FSI(newrgid)))
		bcst.Push(rgid, fmt.Sprintf("create server %s foreign data wrapper hodgepodge_postgres_fdw %s", pg.FSI(newrgid), newrgfsopts))
		bcst.Push(rgid, fmt.Sprintf("insert into hodgepodge.repgroups values (%d, (select oid from pg_foreign_server where srvname = %s))",
			newrgid, pg.FSL(newrgid)))
		// create sudo user mappings
		bcst.Push(newrgid, fmt.Sprintf("drop user mapping if exists for current_user server %s", pg.FSI(rgid)))
		bcst.Push(newrgid, fmt.Sprintf("create user mapping for current_user server %s %s", pg.FSI(rgid), rgumopts))
		bcst.Push(rgid, fmt.Sprintf("drop user mapping if exists for current_user server %s", pg.FSI(newrgid)))
		bcst.Push(rgid, fmt.Sprintf("create user mapping for current_user server %s %s", pg.FSI(newrgid), newrgumopts))
	}
	bcst.Push(newrgid, "select hodgepodge.restore_foreign_tables()")
	_, err = bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}

	// TODO make atomic
	err = cs.PutRepGroups(context.TODO(), rgs)
	if err != nil {
		die("failed to save repgroup data in store: %v", err)
	}
}
