// Copyright (c) 2018, Postgres Professional

package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"go.uber.org/zap"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/pg"
	"postgrespro.ru/shardman/internal/shmnlog"
)

type MoveTask struct {
	SrcRgid    int
	srcConnstr string
	DstRgid    int
	dstConnstr string
	Schema     string
	TableName  string
	Pnum       int
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

// Attempt to clean up after ourselves. We try to leave everything clean and
// consistent at least if rebalance was stopped by signal and all pgs were
// healthy.
func rwcleanup(rwLog *zap.SugaredLogger, src_conn **pgx.Conn, dst_conn **pgx.Conn, task *MoveTask, state int, id int) error {
	var err error
	var report_err error = nil
	rwLog.Debugf("rwcleanup: state is %v", state)
	if state < movePartWorkerConnsEstablished {
		goto CLOSE_CONNS // nothing to do except for closing conns
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("select shardman.write_protection_off(%s::regclass)",
		pg.QL(fmt.Sprintf("%s_%d", task.TableName, task.Pnum))))
	if err != nil {
		rwLog.Errorf("cleanup: write_protection_off failed: %v", err)
		report_err = err
	}

	_, err = (*dst_conn).Exec(fmt.Sprintf("drop subscription if exists hp_copy_%d",
		id))
	if err != nil {
		rwLog.Errorf("cleanup: drop sub failed: %v", err)
		report_err = err
	}

	_, err = (*src_conn).Exec(fmt.Sprintf("drop publication if exists hp_copy_%d",
		id))
	if err != nil {
		rwLog.Errorf("cleanup: drop pub failed: %v", err)
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

	rwLog.Debugf("running cleanup: on dst %s", fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.TableName, task.Pnum))))
	_, err = (*dst_conn).Exec(fmt.Sprintf("drop table if exists %s",
		pg.QI(fmt.Sprintf("%s_%d", task.TableName, task.Pnum))))
	if err != nil {
		rwLog.Errorf("cleanup: drop table failed: %v", err)
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
func movePartWorkerMain(hl *shmnlog.Logger, in <-chan MoveTask, out chan<- report, myid int) {
	var state = movePartWorkerIdle
	var src_conn *pgx.Conn = nil
	var dst_conn *pgx.Conn = nil
	var task MoveTask
	var ok bool
	var sync_lsn string
	var wLog = hl.With("goroutine", "rebalance worker", "id", myid)
	var rwLog *zap.SugaredLogger

	for {
		select {
		case task, ok = <-in:
			if !ok {
				// done, no more tasks, or request to shut down
				if state != movePartWorkerIdle {
					err := rwcleanup(rwLog, &src_conn, &dst_conn, &task, state, myid)
					out <- report{err: err, id: myid}
				}
				break
			}

			if state != movePartWorkerIdle {
				panic("new task came while not idle")
			}
			rwLog = wLog.With("table", task.TableName, "partition num", task.Pnum, "source rgid", task.SrcRgid, "dest rgid", task.DstRgid)
			rwLog.Infof("got new task")
			var connconfig pgx.ConnConfig
			connconfig, err := pgx.ParseConnectionString(task.srcConnstr)
			if err != nil {
				rwLog.Errorf("connstr parse failed: %v", err)
				goto ERR
			}
			src_conn, err = pgx.Connect(connconfig)
			if err != nil {
				rwLog.Errorf("failed to connect to src: %v", err)
				goto ERR
			}
			connconfig, err = pgx.ParseConnectionString(task.dstConnstr)
			if err != nil {
				rwLog.Errorf("connstr parse failed: %v", err)
				goto ERR
			}
			dst_conn, err = pgx.Connect(connconfig)
			if err != nil {
				rwLog.Errorf("failed to connect to dst: %v", err)
				goto ERR
			}
			state = movePartWorkerConnsEstablished
			/* must not broadcast anything */
			_, err = dst_conn.Exec("set session shardman.broadcast_utility to off")
			if err != nil {
				rwLog.Errorf("failed to turn off broadcast_utility: %v", err)
				goto ERR
			}
			_, err = src_conn.Exec("set session shardman.broadcast_utility to off")
			if err != nil {
				rwLog.Errorf("failed to turn off broadcast_utility: %v", err)
				goto ERR
			}

			/* xxx creating indexes afterwards? */
			_, err = dst_conn.Exec(fmt.Sprintf("create table %s (like %s including all)",
				pg.QI(fmt.Sprintf("%s_%d", task.TableName, task.Pnum)),
				pg.QI(task.TableName)))
			if err != nil {
				rwLog.Errorf("dst part creation failed: %v", err)
				goto ERR
			}
			_, err = src_conn.Exec(fmt.Sprintf("create publication hp_copy_%d for table %s",
				myid, pg.QI(fmt.Sprintf("%s_%d", task.TableName, task.Pnum))))
			if err != nil {
				rwLog.Errorf("pub creation failed: %v", err)
				goto ERR
			}
			_, err = dst_conn.Exec(fmt.Sprintf("create subscription hp_copy_%d connection %s publication hp_copy_%d with (synchronous_commit=off)",
				myid, pg.QL(task.srcConnstr), myid))
			if err != nil {
				rwLog.Errorf("sub creation failed: %v", err)
				goto ERR
			}
			state = movePartWorkerWaitInitCopy
			continue

		ERR:
			out <- report{err: err, id: myid}
			err = nil
			rwcleanup(rwLog, &src_conn, &dst_conn, &task, state, myid)
			state = movePartWorkerIdle

		case <-time.After(2 * time.Second):
			var err error

			if state == movePartWorkerWaitInitCopy {
				var synced bool
				err = dst_conn.QueryRow(fmt.Sprintf("select shardman.is_subscription_ready('hp_copy_%d')",
					myid)).Scan(&synced)
				if err != nil {
					rwLog.Errorf("%v", err)
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
					rwLog.Errorf("%v", err)
					goto ERRTMT
				}
				if lag > 1024*1024 {
					continue /* lag is still too big */
				}
				// ok, block writes and wait for full sync
				_, err = src_conn.Exec(fmt.Sprintf("select shardman.write_protection_on(%s::regclass)",
					pg.QL(fmt.Sprintf("%s_%d", task.TableName, task.Pnum))))
				if err != nil {
					rwLog.Errorf("failed to block writes: %v", err)
					goto ERRTMT
				}
				// remember sync lsn
				err = src_conn.QueryRow("select pg_current_wal_lsn()::text").Scan(&sync_lsn)
				if err != nil {
					rwLog.Errorf("failed to get sync lsn: %v", err)
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
					rwLog.Errorf("%v", err)
					goto ERRTMT
				}
				if lag > 0 {
					continue /* not yet */
				}
				// Success. Drop pub and sub (with slot)...
				_, err = dst_conn.Exec(fmt.Sprintf("drop subscription hp_copy_%d", myid))
				if err != nil {
					rwLog.Errorf("%v", err)
					goto ERRTMT
				}
				_, err = src_conn.Exec(fmt.Sprintf("drop publication hp_copy_%d", myid))
				if err != nil {
					rwLog.Errorf("%v", err)
					goto ERRTMT
				}
				// commit metadata update to the store:
				// otherwise we might crash and lost partition.
				// src partition is also dropped here
				state = movePartWorkerCommitting
				_, err = dst_conn.Exec(fmt.Sprintf("select shardman.part_moved(%s::regclass, %d, %d, %d);",
					pg.QL(pg.QI(task.TableName)), task.Pnum, task.SrcRgid, task.DstRgid))
				if err != nil {
					rwLog.Errorf("failed to commit partmove: %v", err)
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
			rwcleanup(rwLog, &src_conn, &dst_conn, &task, state, myid)
			state = movePartWorkerIdle
		}
	}
}

func Rebalance(ctx context.Context, hl *shmnlog.Logger, cs *cluster.ClusterStore, p int, tasks []MoveTask) error {
	// fill connstrs
	connstrs, err := pg.GetSuConnstrs(ctx, cs)
	if err != nil {
		return fmt.Errorf("Failed to get connstrs: %v", err)
	}
	for i, task := range tasks {
		tasks[i].srcConnstr = connstrs[task.SrcRgid]
		tasks[i].dstConnstr = connstrs[task.DstRgid]
	}

	var nworkers = min(p, len(tasks))
	var chans = make(map[int]*movePartWorkerChans)
	var reportch = make(chan report)
	var active_workers = nworkers
	for i := 0; i < nworkers; i++ {
		chans[i] = new(movePartWorkerChans)
		chans[i].in = make(chan MoveTask)
		chans[i].commit = make(chan bool)
		go movePartWorkerMain(hl, chans[i].in, reportch, i)
		chans[i].in <- tasks[len(tasks)-1] // push first task
		tasks = tasks[:len(tasks)-1]
	}

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
				err = report.err
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
	if err != nil {
		hl.Infof("rebalance failed; please run 'select shardman.rebalance_cleanup();' to ensure there is no orphane subs/pubs/slots/partitions")
	}
	return err
}
