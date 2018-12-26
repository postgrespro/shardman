// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
)

var parallelism int

var rebCmd = &cobra.Command{
	Use:   "rebalance",
	Run:   rebalance,
	Short: "Rebalance the data: moves partitions between replication groups until they are evenly distributed. Based on logical replication and performed mostly seamlessly in background; each partition will be only shortly locked in the end to finally sync the data.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if parallelism == 0 || parallelism < -1 {
			hl.Fatalf("Wrong parallelism")
		}
	},
}

func init() {
	rootCmd.AddCommand(rebCmd)

	rebCmd.Flags().IntVarP(&parallelism, "parallelism", "p", 10, "How many partitions to move simultaneously. Moving partitions one-by-one (1) minimizes overhead on cluster operation; -1 means maximum parallelism, all parts are moved at the same time.")
}

type MoveTask struct {
	src_rgid    int
	src_connstr string
	dst_rgid    int
	dst_connstr string
	schema      string
	table_name  string
	pnum        int
}

func rebalance(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	tables, err := pg.GetTables(cs, context.TODO())
	if err != nil {
		hl.Fatalf("Failed to get tables from the store: %v", err)
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		hl.Fatalf("Failed to get repgroups: %v", err)
	}

	var tasks = even_rebalance(tables, rgs)
	if !Rebalance(cs, parallelism, tasks) {
		hl.Fatalf("Something failed, examine the logs")
	}
}

// form slice of tasks giving even rebalance
func even_rebalance(tables []cluster.Table, rgs map[int]*cluster.RepGroup) []MoveTask {
	var tasks = make([]MoveTask, 0)
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
			tasks = append(tasks, MoveTask{
				src_rgid:   fattest_rgid,
				dst_rgid:   leanest_rgid,
				schema:     table.Schema,
				table_name: table.Relname,
				pnum:       moved_part,
			})
			fmt.Printf("Planned moving pnum %d for table %s from rg %d to rg %d\n",
				moved_part, table.Relname, fattest_rgid, leanest_rgid)
			parts_per_rg[leanest_rgid] = append(leanest_parts, moved_part)
			parts_per_rg[fattest_rgid] = fattest_parts[:len(fattest_parts)-1] // pop
			// move part of colocated tables
			for _, ctable := range tables {
				if ctable.ColocateWithSchema == table.Schema &&
					ctable.ColocateWithRelname == table.Relname {
					tasks = append(tasks, MoveTask{
						src_rgid:   fattest_rgid,
						dst_rgid:   leanest_rgid,
						schema:     ctable.Schema,
						table_name: ctable.Relname,
						pnum:       moved_part,
					})
				}
			}
		}
	}
	return tasks
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

type movePartWorkerChans struct {
	in     chan MoveTask
	commit chan bool
}

type commitRequest struct {
	task MoveTask
	id   int
}

type report struct {
	err error // nil ~ ok
	id  int
}

// movepart worker state machine
const (
	movePartWorkerIdle = iota
	movePartWorkerConnsEstablished
	movePartWorkerWaitInitCopy
	movePartWorkerWaitInitialCatchup
	movePartWorkerWaitFullSync
	movePartWorkerCommitting
)

func rwlog(id int, task MoveTask, format string, a ...interface{}) {
	args := []interface{}{id, task.pnum, task.table_name, task.src_rgid, task.dst_rgid}
	args = append(args, a...)
	hl.Infof("Worker %d, moving part %d of table %s from %d to %d: "+format, args...)
}

// Attempt to clean up after ourselves. We try to leave everything clean and
// consistent at least if rebalance was stopped by signal and all pgs were
// healthy.
func rwcleanup(src_conn **pgx.Conn, dst_conn **pgx.Conn, task *MoveTask, state int, id int) error {
	var err error
	var report_err error = nil
	rwlog(id, *task, "rwcleanup: state is %v", state)
	if state < movePartWorkerConnsEstablished {
		goto CLOSE_CONNS // nothing to do except for closing conns
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("select hodgepodge.write_protection_off(%s::regclass)",
		pg.QL(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	if err != nil {
		rwlog(id, *task, "cleanup: write_protection_off failed: %v", err)
		report_err = err
	}

	_, err = (*dst_conn).Exec(fmt.Sprintf("drop subscription if exists hp_copy_%d",
		id))
	if err != nil {
		rwlog(id, *task, "cleanup: drop sub failed: %v", err)
		report_err = err
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("drop publication if exists hp_copy_%d",
		id))
	if err != nil {
		rwlog(id, *task, "cleanup: drop pub failed: %v", err)
		report_err = err
	}

	/*
	 * if we failed during commit, we actually don't know whether it was
	 * successfull or not. So we don't try to remove src part nor dst part;
	 * this should be done in afterwards cleanup
	 */
	if state >= movePartWorkerCommitting {
		goto CLOSE_CONNS
	}

	hl.Infof("running cleanup: on dst %s", fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	_, err = (*dst_conn).Exec(fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	if err != nil {
		rwlog(id, *task, "cleanup: drop table failed: %v", err)
		report_err = err
	}

CLOSE_CONNS:
	// It is safe to close conn multiple times, so it is not neccessary to
	// nullify it, but it is *not* safe to close nil conn, strangely.
	if *src_conn != nil {
		(*src_conn).Close()
	}
	if *dst_conn != nil {
		(*dst_conn).Close()
	}
	return report_err
}

// The communication is simple: worker starts with task, completes it, sends
// report and receives from main worker another one. One exception:
// when in chan is closed (no deadlock risks), worker must exit asap.
func movePartWorkerMain(in <-chan MoveTask, out chan<- report, myid int) {
	var state = movePartWorkerIdle
	var src_conn *pgx.Conn = nil
	var dst_conn *pgx.Conn = nil
	var task MoveTask
	var ok bool
	var sync_lsn string

	for {
		select {
		case task, ok = <-in:
			if !ok {
				// done, no more tasks, or request to shut down
				if state != movePartWorkerIdle {
					err := rwcleanup(&src_conn, &dst_conn, &task, state, myid)
					out <- report{err: err, id: myid}
				}
				break
			}

			if state != movePartWorkerIdle {
				panic("new task came while not idle")
			}
			rwlog(myid, task, "got new task")
			var connconfig pgx.ConnConfig
			connconfig, err := pgx.ParseConnectionString(task.src_connstr)
			if err != nil {
				rwlog(myid, task, "connstr parse failed: %v", err)
				goto ERR
			}
			src_conn, err = pgx.Connect(connconfig)
			if err != nil {
				rwlog(myid, task, "failed to connect to src: %v", err)
				goto ERR
			}
			connconfig, err = pgx.ParseConnectionString(task.dst_connstr)
			if err != nil {
				rwlog(myid, task, "connstr parse failed: %v", err)
				goto ERR
			}
			dst_conn, err = pgx.Connect(connconfig)
			if err != nil {
				rwlog(myid, task, "failed to connect to dst: %v", err)
				goto ERR
			}
			state = movePartWorkerConnsEstablished
			/* must not broadcast anything */
			_, err = dst_conn.Exec("set session hodgepodge.broadcast_utility to off")
			if err != nil {
				rwlog(myid, task, "failed to turn off broadcast_utility: %v", err)
				goto ERR
			}
			_, err = src_conn.Exec("set session hodgepodge.broadcast_utility to off")
			if err != nil {
				rwlog(myid, task, "failed to turn off broadcast_utility: %v", err)
				goto ERR
			}

			/* xxx creating indexes afterwards? */
			_, err = dst_conn.Exec(fmt.Sprintf("create table %s (like %s including all)",
				pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum)),
				pg.QI(task.table_name)))
			if err != nil {
				rwlog(myid, task, "dst part creation failed: %v", err)
				goto ERR
			}
			_, err = src_conn.Exec(fmt.Sprintf("create publication hp_copy_%d for table %s",
				myid, pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
			if err != nil {
				rwlog(myid, task, "pub creation failed: %v", err)
				goto ERR
			}
			_, err = dst_conn.Exec(fmt.Sprintf("create subscription hp_copy_%d connection %s publication hp_copy_%d with (synchronous_commit=off)",
				myid, pg.QL(task.src_connstr), myid))
			if err != nil {
				rwlog(myid, task, "sub creation failed: %v", err)
				goto ERR
			}
			state = movePartWorkerWaitInitCopy
			continue

		ERR:
			out <- report{err: err, id: myid}
			err = nil
			rwcleanup(&src_conn, &dst_conn, &task, state, myid)
			state = movePartWorkerIdle

		case <-time.After(2 * time.Second):
			var err error

			if state == movePartWorkerWaitInitCopy {
				var synced bool
				err = dst_conn.QueryRow(fmt.Sprintf("select hodgepodge.is_subscription_ready('hp_copy_%d')",
					myid)).Scan(&synced)
				if err != nil {
					rwlog(myid, task, "%v", err)
					goto ERRTMT
				}
				if synced {
					state = movePartWorkerWaitInitialCatchup
				}
				continue
			}
			if state == movePartWorkerWaitInitialCatchup {
				var lag int64
				err = src_conn.QueryRow(fmt.Sprintf("select pg_current_wal_lsn() - confirmed_flush_lsn from pg_replication_slots where slot_name='hp_copy_%d'",
					myid)).Scan(&lag)
				if err != nil {
					rwlog(myid, task, "%v", err)
					goto ERRTMT
				}
				if lag > 1024*1024 {
					continue /* lag is still too big */
				}
				// ok, block writes and wait for full sync
				_, err = src_conn.Exec(fmt.Sprintf("select hodgepodge.write_protection_on(%s::regclass)",
					pg.QL(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
				if err != nil {
					rwlog(myid, task, "failed to block writes: %v", err)
					goto ERRTMT
				}
				// remember sync lsn
				err = src_conn.QueryRow("select pg_current_wal_lsn()::text").Scan(&sync_lsn)
				if err != nil {
					rwlog(myid, task, "failed to get sync lsn: %v", err)
					goto ERRTMT
				}
				state = movePartWorkerWaitFullSync
				continue
			}
			if state == movePartWorkerWaitFullSync {
				var lag int64
				err = src_conn.QueryRow(fmt.Sprintf("select %s - confirmed_flush_lsn from pg_replication_slots where slot_name='hp_copy_%d'",
					pg.QL(sync_lsn), myid)).Scan(&lag)
				if err != nil {
					rwlog(myid, task, "%v", err)
					goto ERRTMT
				}
				if lag > 0 {
					continue /* not yet */
				}
				// Success. Drop pub and sub (with slot)...
				_, err = dst_conn.Exec(fmt.Sprintf("drop subscription hp_copy_%d", myid))
				if err != nil {
					rwlog(myid, task, "%v", err)
					goto ERRTMT
				}
				_, err = src_conn.Exec(fmt.Sprintf("drop publication hp_copy_%d", myid))
				if err != nil {
					rwlog(myid, task, "%v", err)
					goto ERRTMT
				}
				// commit metadata update to the store:
				// otherwise we might crash and lost partition.
				// src partition is also dropped here
				state = movePartWorkerCommitting
				_, err = dst_conn.Exec(fmt.Sprintf("select hodgepodge.part_moved(%s::regclass, %d, %d, %d);",
					pg.QL(pg.QI(task.table_name)), task.pnum, task.src_rgid, task.dst_rgid))
				if err != nil {
					rwlog(myid, task, "failed to commit partmove: %v", err)
					goto ERRTMT
				}

				/* done */
				out <- report{err: nil, id: myid}
				state = movePartWorkerIdle
				continue
			}
			// movePartWorkerIdle or movePartWorkerWaitCommit,
			// nothing to do (yet or already)
			continue

		ERRTMT:
			out <- report{err: err, id: myid}
			err = nil
			rwcleanup(&src_conn, &dst_conn, &task, state, myid)
			state = movePartWorkerIdle
		}
	}
}

// Separate func to be able to call from other cmds. Returns true if everything
// is ok.
func Rebalance(cs *cluster.ClusterStore, p int, tasks []MoveTask) bool {
	// fill connstrs
	connstrs, err := pg.GetSuConnstrs(context.TODO(), cs)
	if err != nil {
		hl.Fatalf("Failed to get connstrs: %v", err)
	}
	for i, task := range tasks {
		tasks[i].src_connstr = connstrs[task.src_rgid]
		tasks[i].dst_connstr = connstrs[task.dst_rgid]
	}

	var nworkers = min(p, len(tasks))
	var chans = make(map[int]*movePartWorkerChans)
	var reportch = make(chan report)
	var active_workers = nworkers
	for i := 0; i < nworkers; i++ {
		chans[i] = new(movePartWorkerChans)
		chans[i].in = make(chan MoveTask)
		chans[i].commit = make(chan bool)
		go movePartWorkerMain(chans[i].in, reportch, i)
		chans[i].in <- tasks[len(tasks)-1] // push first task
		tasks = tasks[:len(tasks)-1]
	}
	var ok = true
	var sigs = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	var stopped = false
	// We must continue looping until we get reports from all workers even
	// after we get shutdown signal because some of them might be hanging
	// in sending something to us
	for active_workers != 0 {
		select {
		case report := <-reportch:
			if report.err != nil {
				ok = false
				// not much sense to continue after any error
				if !stopped {
					for i := 0; i < nworkers; i++ {
						close(chans[i].in)
					}
					stopped = true
				}
			}
			if stopped {
				active_workers--
				continue
			}
			if len(tasks) != 0 { // push next task
				chans[report.id].in <- tasks[len(tasks)-1]
				tasks = tasks[:len(tasks)-1]
			} else { // or shut down worker
				close(chans[report.id].in)
				active_workers--
			}

		case _ = <-sigs: // stop all workers immediately
			hl.Infof("Stopping all workers")
			for i := 0; i < nworkers; i++ {
				close(chans[i].in)
			}
			stopped = true
		}
	}
	if !ok {
		hl.Infof("rebalance failed; please run 'select hodgepodge.rebalance_cleanup();' to ensure there is no orphane subs/pubs/slots/partitions")
	}
	return ok
}
