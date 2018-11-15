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

var parallelism int

var rebCmd = &cobra.Command{
	Use:   "rebalance",
	Run:   rebalance,
	Short: "Rebalance the data: moves partitions between replication groups until they are evenly distributed. Based on logical replication and performed mostly seamlessly in background; each partition will be only shortly locked in the end to finally sync the data.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if parallelism == 0 || parallelism < -1 {
			die("Wrong parallelism")
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
	table_name  string
	pnum        int
}

func rebalance(cmd *cobra.Command, args []string) {
	cs, err := cmdcommon.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()

	tables, _, err := cs.GetTables(context.TODO())
	if err != nil {
		die("Failed to get tables from the store: %v", err)
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("Failed to get repgroups: %v", err)
	}

	var tasks = even_rebalance(tables, rgs)
	if !Rebalance(cs, parallelism, tasks) {
		die("Something failed, examine the logs")
	}
}

// form slice of tasks giving even rebalance
func even_rebalance(tables []cluster.Table, rgs map[int]*cluster.RepGroup) []MoveTask {
	var tasks = make([]MoveTask, 0)
	for _, table := range tables {
		if table.ColocateWith != "" {
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
				die("Metadata is broken: partholder %d is non-existing rgid", rgid)
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
				table_name: table.Relname,
				pnum:       moved_part,
			})
			fmt.Printf("Planned moving pnum %d for table %s from rg %d to rg %d\n",
				moved_part, table.Relname, fattest_rgid, leanest_rgid)
			parts_per_rg[leanest_rgid] = append(leanest_parts, moved_part)
			parts_per_rg[fattest_rgid] = fattest_parts[:len(fattest_parts)-1] // pop
			// move part of colocated tables
			for _, ctable := range tables {
				if ctable.ColocateWith == table.Relname {
					tasks = append(tasks, MoveTask{
						src_rgid:   fattest_rgid,
						dst_rgid:   leanest_rgid,
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
	movePartWorkerWaitCommit
)

func rwlog(id int, task MoveTask, format string, a ...interface{}) {
	args := []interface{}{id, task.pnum, task.table_name, task.src_rgid, task.dst_rgid}
	args = append(args, a...)
	log.Printf("Worker %d, moving part %d of table %s from %d to %d: "+format, args...)
}

// Attempt to clean up after ourselves, assuming task didn't commit. We try to
// leave everything clean and consistent at least if rebalance was stopped by
// signal and all pgs were healthy.
func rwcleanup(src_conn **pgx.Conn, dst_conn **pgx.Conn, task *MoveTask, state int, id int) {
	var err error
	log.Printf("state is %v", state)
	if state < movePartWorkerConnsEstablished {
		goto CLOSE_CONNS // nothing to do except for closing conns
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("select hodgepodge.write_protection_off(%s::regclass)",
		pg.QL(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	if err != nil {
		rwlog(id, *task, "cleanup: write_protection_off failed: %v", err)
	}

	_, err = (*dst_conn).Exec(fmt.Sprintf("drop subscription if exists hp_copy_%d",
		id))
	if err != nil {
		rwlog(id, *task, "cleanup: drop sub failed: %v", err)
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("drop publication if exists hp_copy_%d",
		id))
	if err != nil {
		rwlog(id, *task, "cleanup: drop pub failed: %v", err)
	}

	log.Printf("running cleanup: on dst %s", fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	_, err = (*dst_conn).Exec(fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
	if err != nil {
		rwlog(id, *task, "cleanup: drop table failed: %v", err)
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
}

// when shutdown chan is closed, we must exit asap
// when part is moved, we ask main goroutine to commit the change through out
// chan; he responds to us via commit chan -- true if successfully put.
func movePartWorkerMain(in <-chan MoveTask, commit <-chan bool, shutdown chan struct{}, out chan<- interface{}, myid int) {
	var state = movePartWorkerIdle
	var src_conn *pgx.Conn = nil
	var dst_conn *pgx.Conn = nil
	var task MoveTask
	var ok bool
	var sync_lsn string

	for {
		select {
		case task, ok = <-in:
			if state != movePartWorkerIdle {
				panic("new task came while not idle")
			}
			if !ok { // done, no more tasks
				break
			}
			log.Printf("Worker %d: got new task: table %s, pnum %d %d->%d",
				myid, task.table_name, task.pnum, task.src_rgid, task.dst_rgid)
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

		case <-shutdown: // shutdown request
			// If main waits from us, push the report and exit;
			// otherwise he won't know when we are finished.
			// If we wait from main, then just exit right away: main
			// won't send us anything after getting shutdown;
			// commitRequest is assumed failed in this case.
			// We could close 'in' channel instead of using separate
			// chan, but this lets spread logic a bit.
			// Note that here we rely on the fact that 'in' channel is
			// unbuffered; the following is impossible:
			// -- worker sends commitRequest
			// -- main commits and puts commit to 'in' chan
			// -- main gots shutdown and notifies all workers
			// -- worker gets shutdown before commit ack and aborts
			// XXX in this schema we have no way of telling main that
			// rollback failed
			if !(state == movePartWorkerWaitCommit || state == movePartWorkerIdle) {
				// main waits from us
				out <- report{err: nil, id: myid}
			}
			rwcleanup(&src_conn, &dst_conn, &task, state, myid)
			break

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
				// but before dropping source table, we must
				// commit metadata update to the store:
				// otherwise we might crash and lost partition.
				// So signal that we want commit
				out <- commitRequest{task: task, id: myid}
				state = movePartWorkerWaitCommit
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

		case ok := <-commit:
			var err error
			if state != movePartWorkerWaitCommit {
				panic("commit came while not in movePartWorkerWaitCommit")
			}

			if ok {
				// Now we can drop source part
				_, err = src_conn.Exec(fmt.Sprintf("drop table %s",
					pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
				if err != nil {
					rwlog(myid, task, "failed to drop src part: %v", err)
				}
			} else {
				// commit to the store failed; unlock the source
				// and drop the dest
				_, err = src_conn.Exec(fmt.Sprintf("select hodgepodge.write_protection_off(%s::regclass)",
					pg.QL(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
				if err != nil {
					rwlog(myid, task, "write_protection_off failed: %v", err)
					goto CMTDONE
				}

				_, err = dst_conn.Exec(fmt.Sprintf("drop table %s",
					pg.QI(fmt.Sprintf("%s_%d", task.table_name, task.pnum))))
				if err != nil {
					rwlog(myid, task, "failed to drop dst part: %v", err)
				}
			}
		CMTDONE:
			out <- report{err: err, id: myid}
			err = nil
			state = movePartWorkerIdle
			src_conn.Close()
			dst_conn.Close()
		}
	}
}

// Separate func to be able to call from other cmds. Returns true if everything
// is ok.
func Rebalance(cs store.ClusterStore, p int, tasks []MoveTask) bool {
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

	// fill connstrs
	connstrs, err := pg.GetSuConnstrs(context.TODO(), rgs, cldata)
	if err != nil {
		die("Failed to get connstrs: %v", err)
	}
	for i, task := range tasks {
		tasks[i].src_connstr = connstrs[task.src_rgid]
		tasks[i].dst_connstr = connstrs[task.dst_rgid]
	}

	var nworkers = min(p, len(tasks))
	var chans = make(map[int]*movePartWorkerChans)
	var reportch = make(chan interface{})
	var shutdownch = make(chan struct{})
	var active_workers = nworkers
	for i := 0; i < nworkers; i++ {
		chans[i] = new(movePartWorkerChans)
		chans[i].in = make(chan MoveTask)
		chans[i].commit = make(chan bool)
		go movePartWorkerMain(chans[i].in, chans[i].commit, shutdownch, reportch, i)
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
MainLoop:
	for active_workers != 0 {
		select {
		case msg := <-reportch:
			switch msg := msg.(type) {
			case report:
				if msg.err != nil {
					ok = false
				}
				if stopped {
					active_workers--
					continue
				}
				if len(tasks) != 0 { // push next task
					chans[msg.id].in <- tasks[len(tasks)-1]
					tasks = tasks[:len(tasks)-1]
				} else { // or shut down worker
					close(chans[msg.id].in)
					active_workers--
				}
			case commitRequest:
				if stopped {
					active_workers--
					continue
				}
				tables, _, err := cs.GetTables(context.TODO())
				if err != nil {
					log.Printf("Failed to get tables from the store: %v", err)
					chans[msg.id].commit <- false
					continue
				}

				for _, table := range tables {
					if table.Relname == msg.task.table_name {
						if table.Partmap[msg.task.pnum] != msg.task.src_rgid {
							log.Printf("According to the store, moved part %s of table %s was on rg %d, not on %d",
								msg.task.pnum, table.Relname, table.Partmap[msg.task.pnum], msg.task.src_rgid)
							chans[msg.id].commit <- false
							continue MainLoop
						}
						table.Partmap[msg.task.pnum] = msg.task.dst_rgid
						err = cs.PutTables(context.TODO(), tables)
						if err != nil {
							log.Printf("failed to save tables data in store: %v", err)
							// FIXME: there's a gigantic hole here. If we actually *succeeded* in committing
							// update, but failed to get confirmation, worker would drop src partition, effectively
							// losing it.
							chans[msg.id].commit <- false
							continue MainLoop
						}
						log.Printf("Committed part %d move of table %s from %d to %d",
							msg.task.pnum, table.Relname, msg.task.src_rgid, msg.task.dst_rgid)
						chans[msg.id].commit <- true
						continue MainLoop
					}
				}
				log.Printf("Table %v with moved part not found in the store", msg.task.table_name)
				chans[msg.id].commit <- false
			}

		case _ = <-sigs: // stop all workers immediately
			log.Printf("Stopping all workers")
			for i := 0; i < nworkers; i++ {
				close(shutdownch)
			}
			stopped = true
		}
	}

	return ok
}
