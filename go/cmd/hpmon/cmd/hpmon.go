// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"

	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

// Here we will store args
var cfg cmdcommon.CommonConfig

var hpmonCmd = &cobra.Command{
	Use:   "hpmon",
	Short: "Hodgepodge monitor. Ensures that all replication groups are aware of current partitions positions. Running several instances is safe.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := cmdcommon.CheckConfig(&cfg); err != nil {
			log.Fatalf(err.Error())
		}
	},
	Run: hpmon,
}

// Entry point
func Execute() {
	if err := hpmonCmd.Execute(); err != nil {
		log.Fatalf(err.Error())
	}
}

func init() {
	cmdcommon.AddCommonFlags(hpmonCmd, &cfg)
}

type hpMonState struct {
	cs      store.ClusterStore
	ctx     context.Context
	workers map[int]chan ClusterState // rgid is the key
}

// what is sharded and current masters, fed into mon worker
type ClusterState struct {
	tables      []cluster.Table
	connstrmaps map[int]map[string]string
}

func hpmon(c *cobra.Command, args []string) {
	var state hpMonState
	state.workers = make(map[int]chan ClusterState)

	ctx, cancel := context.WithCancel(context.Background())
	state.ctx = ctx
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// TODO: watch instead of polling
	reloadStoreTimerCh := time.NewTimer(0).C
	log.Printf("hpmon started")
	for {
		select {
		case <-sigs:
			cancel()
			log.Printf("stopping hpmon")
			return

		case <-reloadStoreTimerCh:
			reloadStore(&state)
			reloadStoreTimerCh = time.NewTimer(5 * time.Second).C
		}
	}
}

func reloadStore(state *hpMonState) {
	var err error
	var clstate ClusterState
	var tables []cluster.Table
	var rgs map[int]*cluster.RepGroup
	connstrmaps := make(map[int]map[string]string)

	if state.cs == nil {
		state.cs, err = cmdcommon.NewClusterStore(&cfg)
		if err != nil {
			log.Printf("Failed to create store: %v", err)
			return
		}
	}
	cldata, _, err := state.cs.GetClusterData(state.ctx)
	if err != nil {
		log.Printf("cannot get cluster data: %v", err)
		// reset store, probably connection failure
		goto StoreError
	}

	rgs, _, err = state.cs.GetRepGroups(state.ctx)
	if err != nil {
		log.Printf("Failed to get repgroups: %v", err)
		goto StoreError
	}
	// shut down workers for removed repgroups
	for rgid, in := range state.workers {
		if _, ok := rgs[rgid]; !ok {
			close(in)
			delete(state.workers, rgid)
		}
	}
	// spin up workers for new repgroups
	for rgid, _ := range rgs {
		if _, ok := state.workers[rgid]; !ok {
			// 1 size to allow
			state.workers[rgid] = make(chan ClusterState, 1)
			go monWorkerMain(state.ctx, rgid, state.workers[rgid])
		}
	}

	// learn connstrs
	for rgid, rg := range rgs {
		connstrmaps[rgid], err = store.GetSuConnstrMap(state.ctx, rg, cldata)
		if err != nil {
			log.Printf("Failed to get connstr for rgid %d: %v", rgid, err)
			return
		}
	}

	tables, _, err = state.cs.GetTables(state.ctx)
	if err != nil {
		log.Printf("Failed to get tables from the store: %v", err)
		goto StoreError
	}

	// Send current state to all workers. They must not scribble on it.
	clstate = ClusterState{tables: tables, connstrmaps: connstrmaps}
	for _, in := range state.workers {
		in <- clstate
	}

	return
StoreError:
	state.cs.Close()
	state.cs = nil
	return
}

type monWorker struct {
	ctx     context.Context
	rgid    int
	clstate ClusterState
	conn    *pgx.Conn
	connstr string
	// triggers retry if previous attempt failed
	retryTimer *time.Timer
}

func (w *monWorker) log(format string, a ...interface{}) {
	args := []interface{}{w.rgid}
	args = append(args, a...)
	log.Printf("Mon worker %d: "+format, args...)
}

// each monworker serves one repgroup
func monWorkerMain(ctx context.Context, rgid int, in chan ClusterState) {
	var w = monWorker{
		ctx:        ctx,
		rgid:       rgid,
		conn:       nil,
		retryTimer: time.NewTimer(0),
	}
	w.retryTimer.Stop()
	w.log("Starting")

	for {
		select {
		case <-ctx.Done():
			if w.conn != nil {
				w.conn.Close()
			}
			w.log("stopped")
			return

		case clstate, ok := <-in:
			if !ok {
				w.log("exit")
				return
			}
			w.clstate = clstate
			// if connstr changed, invalidate connection
			newconnstr := pg.ConnString(clstate.connstrmaps[rgid])
			if w.conn != nil && newconnstr != w.connstr {
				w.conn.Close()
			}
			w.connstr = newconnstr
			monWorkerFull(&w)
		}
	}
}

// actually perform full cycle: fix user mappings, foreign servers, partitions
func monWorkerFull(w *monWorker) {
	var err error
	if w.conn == nil {
		connconfig, err := pgx.ParseConnectionString(w.connstr)
		if err != nil {
			w.log("failed to parse connstr %s: %v", w.connstr, err)
			return
		}
		w.conn, err = pgx.Connect(connconfig)
		if err != nil {
			w.log("unable to connect to database: %v", err)
			w.retryTimer = time.NewTimer(5 * time.Second)
			return
		}
	}
	for rgid, connstrmap := range w.clstate.connstrmaps {
		if rgid == w.rgid {
			continue
		}

		// foreign server
		rows, err := w.conn.Query(fmt.Sprintf(
			"select split_part(opt, '=', 1) k, split_part(opt, '=', 2) v from (select unnest(srvoptions) opt from pg_foreign_server where srvname = 'hp_rg_%d') o;",
			rgid))
		if err != nil {
			w.log("failed to retrieve fserver info: %v", err)
			goto ConnError
		}
		var currfsoptsmap = make(map[string]string)
		var key, value, currfsopts, newfsopts string
		for rows.Next() {
			err = rows.Scan(&key, &value)
			if err != nil {
				w.log("%v", err)
				goto ConnError
			}
			currfsoptsmap[key] = value
		}
		if rows.Err() != nil {
			w.log("%v", rows.Err())
			goto ConnError
		}
		newfsopts, _ = pg.FormForeignServerOpts(connstrmap)
		currfsopts, err = pg.FormForeignServerOpts(currfsoptsmap)
		if err != nil || currfsopts != newfsopts {
			// Need to recreate foreign server
			w.log("Recreating foreign server to rg %d", rgid)
			_, err = w.conn.Exec(fmt.Sprintf(
				"drop server if exists hp_rg_%d cascade", rgid))
			if err != nil {
				w.log("%v", err)
				goto ConnError
			}
			_, err = w.conn.Exec(fmt.Sprintf(
				"create server hp_rg_%d foreign data wrapper postgres_fdw %s",
				rgid, newfsopts))
			if err != nil {
				w.log("%v", err)
				goto ConnError
			}

		}

		// user mapping
		rows, err = w.conn.Query(fmt.Sprintf(
			`select split_part(opt, '=', 1) k, split_part(opt, '=', 2) v from
(select unnest(umoptions) opt from pg_user_mapping um, pg_foreign_server fs where fs.srvname = 'hp_rg_%d' and fs.oid = um.umserver) o;`,
			rgid))
		if err != nil {
			w.log("failed to retrieve um info: %v", err)
			goto ConnError
		}
		var currumoptsmap = make(map[string]string)
		var currumopts, newumopts string
		for rows.Next() {
			err = rows.Scan(&key, &value)
			if err != nil {
				w.log("%v", err)
				goto ConnError
			}
			currumoptsmap[key] = value
		}
		if rows.Err() != nil {
			w.log("%v", rows.Err())
			goto ConnError
		}
		newumopts, _ = pg.FormUserMappingOpts(connstrmap)
		currumopts, err = pg.FormUserMappingOpts(currumoptsmap)
		if err != nil || currumopts != newumopts {
			// Need to recreate user mapping
			w.log("Recreating user mapping to rg %d", rgid)
			_, err = w.conn.Exec(fmt.Sprintf(
				"drop user mapping if exists for current_user server hp_rg_%d",
				rgid))
			if err != nil {
				w.log("%v", err)
				goto ConnError
			}
			_, err = w.conn.Exec(fmt.Sprintf(
				"create user mapping for current_user server hp_rg_%d %s",
				rgid, newumopts))
		}
	}

	for _, table := range w.clstate.tables {
		for pnum, holder := range table.Partmap {
			var part_in_tree bool
			err = w.conn.QueryRow(fmt.Sprintf(
				"select exists(select 1 from pg_inherits where inhparent::regclass::name = %s and inhrelid::regclass::name = %s)",
				pg.QL(table.Relname), pg.PL(table.Relname, pnum))).Scan(&part_in_tree)
			if err != nil {
				w.log("Failed to check part in tree %v", err)
				goto ConnError
			}
			if holder == w.rgid { // I am the holder
				if part_in_tree {
					continue // ok
				}
				w.log("Attaching real partition %s", pg.P(table.Relname, pnum))
				_, err = w.conn.Exec(fmt.Sprintf(
					"drop foreign table if exists %s cascade",
					pg.FPI(table.Relname, pnum)))
				if err != nil {
					w.log("%v", err)
					goto ConnError
				}
				_, err = w.conn.Exec(fmt.Sprintf(
					"alter table %s attach partition %s for values with (modulus %d, remainder %d)",
					pg.QI(table.Relname), pg.PI(table.Relname, pnum), table.Nparts, pnum))
				if err != nil {
					w.log("%v", err)
					goto ConnError
				}
			} else { // part is foreign
				// detach real part, if it is in tree
				if part_in_tree {
					_, err = w.conn.Exec(fmt.Sprintf(
						"alter table %s detach partition %s",
						pg.QI(table.Relname), pg.PI(table.Relname, pnum)))
					if err != nil {
						w.log("%v", err)
						goto ConnError
					}
				}

				var ftable_ok bool // ftable in tree and points to proper server
				err = w.conn.QueryRow(fmt.Sprintf(
					`select exists(select 1 from pg_inherits i, pg_foreign_table ft, pg_foreign_server fs
where i.inhparent::regclass::name = %s and i.inhrelid::regclass::name = %s and i.inhrelid = ft.ftrelid
and fs.oid = ft.ftserver and fs.srvname = %s)`,
					pg.QL(table.Relname), pg.FPL(table.Relname, pnum), pg.FSL(table.Partmap[pnum]))).Scan(&ftable_ok)
				if err != nil {
					w.log("Failed to check fdw part in tree %v", err)
					goto ConnError
				}
				if ftable_ok {
					continue // ok
				}

				w.log("Recreating foreign partition %s pointing to rg %d", pg.FP(table.Relname, pnum), table.Partmap[pnum])
				_, err = w.conn.Exec(fmt.Sprintf(
					"drop foreign table if exists %s cascade",
					pg.FPI(table.Relname, pnum)))
				if err != nil {
					w.log("%v", err)
					goto ConnError
				}

				_, err = w.conn.Exec(fmt.Sprintf(
					"create foreign table %s partition of %s for values with (modulus %d, remainder %d) server %s options (table_name %s)",
					pg.FPI(table.Relname, pnum), pg.QI(table.Relname), table.Nparts, pnum, pg.FSI(table.Partmap[pnum]), pg.PL(table.Relname, pnum)))
				if err != nil {
					w.log("%v", err)
					goto ConnError
				}
			}
		}
	}

	w.retryTimer.Stop() // all is ok, disable retry timer
	return
ConnError:
	w.conn.Close()
	w.conn = nil
	w.retryTimer = time.NewTimer(5 * time.Second)
}
