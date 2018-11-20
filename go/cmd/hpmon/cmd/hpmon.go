// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
var noXR bool
var noDD bool
var checkDeadlockIntervalRaw string
var checkDeadlockInterval time.Duration

var hpmonCmd = &cobra.Command{
	Use: "hpmon",
	Short: `Hodgepodge monitor. It
  * Ensures that all replication groups are aware of current partitions positions.
  * Resolves 2PC (distributed) transactions.
  * Resolves deadlocks.
Running several instances is safe.
`,
	PersistentPreRun: func(c *cobra.Command, args []string) {
		var err error
		if err = cmdcommon.CheckConfig(&cfg); err != nil {
			log.Fatalf(err.Error())
		}
		checkDeadlockInterval, err = parseDuration(checkDeadlockIntervalRaw)
		if err != nil {
			log.Fatalf(fmt.Sprintf("wrong deadlock interval: %v", err.Error()))
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
	hpmonCmd.PersistentFlags().BoolVar(&noXR, "no-xact-resolver", false, "don't run xact resolver")
	hpmonCmd.PersistentFlags().BoolVar(&noDD, "no-deadlock-detector", false, "don't run deadlock detector")
	hpmonCmd.PersistentFlags().StringVar(&checkDeadlockIntervalRaw, "deadlock-timeout", "2s", "interval between deadlock checks. Accepted formats are the same as in PostgreSQL's GUCs; default unit is ms, as in PG's deadlock_timeout")

	// randomize seed
	rand.Seed(time.Now().Unix())
}

var durationRegexp = regexp.MustCompile(`^([0-9]+)[\s]*(ms|s|min|h|d|)$`)

func parseDuration(raw string) (time.Duration, error) {
	var modifier time.Duration
	matches := durationRegexp.FindStringSubmatch(raw)
	if len(matches) < 2 {
		return 0, fmt.Errorf("Failed to parse duration %s", raw)
	}
	var n, _ = strconv.ParseInt(matches[1], 10, 64)
	switch matches[2] {
	case "ms", "":
		modifier = time.Millisecond
	case "s":
		modifier = time.Second
	case "m":
		modifier = time.Minute
	case "h":
		modifier = time.Hour
	case "d":
		modifier = time.Hour * 24
	}
	return modifier * time.Duration(n), nil
}

type hpMonState struct {
	cs                  store.ClusterStore
	ctx                 context.Context
	workers             map[int]chan clusterState // rgid is the key
	xact_resolverch     chan clusterState
	deadlock_detectorch chan clusterState
	wg                  sync.WaitGroup
}

// what is sharded and current masters, fed into workers
type clusterState struct {
	tables []cluster.Table
	rgs    map[int]*repGroupState
}
type repGroupState struct {
	sysId      int64
	connstrmap map[string]string
}

// TODO: we should we add ctx to all pg's commands to prevent any worker
// hanging, blocking everything
func hpmon(c *cobra.Command, args []string) {
	var state hpMonState
	state.workers = make(map[int]chan clusterState)
	state.xact_resolverch = make(chan clusterState)
	state.deadlock_detectorch = make(chan clusterState)

	ctx, cancel := context.WithCancel(context.Background())
	state.ctx = ctx
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start xact resolver and deadlock detector
	if !noXR {
		state.wg.Add(1)
		go xactResolverMain(ctx, state.xact_resolverch, &state.wg)
	}
	if !noDD {
		state.wg.Add(1)
		go deadlockDetectorMain(ctx, state.deadlock_detectorch, &state.wg)
	}

	// TODO: watch instead of polling
	reloadStoreTimerCh := time.NewTimer(0).C
	log.Printf("hpmon started")
	for {
		select {
		case <-sigs:
			cancel()
			log.Printf("stopping hpmon")
			state.wg.Wait()
			return

		case <-reloadStoreTimerCh:
			reloadStore(&state)
			reloadStoreTimerCh = time.NewTimer(5 * time.Second).C
		}
	}
}

func reloadStore(state *hpMonState) {
	var err error
	var rgs map[int]*cluster.RepGroup
	var clstate = clusterState{rgs: make(map[int]*repGroupState)}

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
			state.wg.Add(1)
			state.workers[rgid] = make(chan clusterState)
			go monWorkerMain(state.ctx, rgid, state.workers[rgid], &state.wg)
		}
	}

	// learn connstrs
	for rgid, rg := range rgs {
		connstrmap, err := store.GetSuConnstrMap(state.ctx, rg, cldata)
		if err != nil {
			log.Printf("Failed to get connstr for rgid %d: %v", rgid, err)
			return
		}
		clstate.rgs[rgid] = &repGroupState{connstrmap: connstrmap, sysId: rg.SysId}
	}

	clstate.tables, _, err = state.cs.GetTables(state.ctx)
	if err != nil {
		log.Printf("Failed to get tables from the store: %v", err)
		goto StoreError
	}

	// Send current state to all workers. They must not scribble on it.
	for _, in := range state.workers {
		in <- clstate
	}
	if !noXR {
		state.xact_resolverch <- clstate // push it to xact resolver also
	}
	if !noDD {
		state.deadlock_detectorch <- clstate // push it to deadlock detector also
	}

	return
StoreError:
	state.cs.Close()
	state.cs = nil
	return
}

type monWorker struct {
	rgid    int
	clstate clusterState
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

const retryConnInterval = 2 * time.Second

// each monworker serves one repgroup
func monWorkerMain(ctx context.Context, rgid int, in <-chan clusterState, wg *sync.WaitGroup) {
	defer wg.Done()
	var w = monWorker{
		rgid:       rgid,
		conn:       nil,
		retryTimer: time.NewTimer(0),
	}
	<-w.retryTimer.C
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
				if w.conn != nil {
					w.conn.Close()
				}
				w.log("exit")
				return
			}
			w.clstate = clstate
			// if connstr changed, invalidate connection
			newconnstr := pg.ConnString(clstate.rgs[rgid].connstrmap)
			if w.conn != nil && newconnstr != w.connstr {
				w.conn.Close()
				w.conn = nil
			}
			w.connstr = newconnstr
			monWorkerFull(&w)

		case <-w.retryTimer.C:
			monWorkerFull(&w)
		}
	}
}

// actually perform full cycle: fix user mappings, foreign servers, partitions
func monWorkerFull(w *monWorker) {
	// w.log("DEBUG: monWorkerFull")
	var err error
	if w.conn == nil {
		connconfig, err := pgx.ParseConnectionString(w.connstr)
		if err != nil {
			w.log("failed to parse connstr %s: %v", w.connstr, err)
			// no point to retry until new connstr arrives
			w.retryTimer = time.NewTimer(0)
			<-w.retryTimer.C
			return
		}
		w.conn, err = pgx.Connect(connconfig)
		if err != nil {
			w.log("unable to connect to database: %v", err)
			w.retryTimer = time.NewTimer(retryConnInterval)
			return
		}
	}
	for rgid, rg := range w.clstate.rgs {
		if rgid == w.rgid {
			continue
		}
		connstrmap := rg.connstrmap

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

	// all is ok, defuse timer
	// recommended "if !t.Stop() { <-t }" doesn't work because if timer
	// was previously stopped, we hang in <-t
	w.retryTimer = time.NewTimer(0)
	<-w.retryTimer.C
	return
ConnError:
	w.conn.Close()
	w.conn = nil
	w.retryTimer = time.NewTimer(retryConnInterval)
}

func xrm_log(format string, a ...interface{}) {
	log.Printf("Xact resolver main: "+format, a...)
}

type xResolveRequest struct {
	requester int // requester rgid
	gid       string
}

type xStatusRequest struct {
	gid string
}

const (
	xStatusCommitted = iota
	xStatusAborted
	xStatusInProgress
	xStatusUnknown
	xStatusFailedToCheck
)

type xStatusResponse struct {
	gid     string
	xStatus int
}

type xResolveResponse struct {
	gid     string
	xStatus int
}

type xactResolverState struct {
	xrworkers             map[int]chan interface{} // channels to workers
	clstate               clusterState
	resolve_requests      map[string][]int // gid -> list of requesters
	in                    chan interface{} // workers put their msg here
	retryBcstClstateTimer *time.Timer
}

const xactResolverWorkerChanBuf = 100
const retryBcstClstateInterval = 1 * time.Second

// Performs resolution of distributed xacts. Since each instance is backed up by
// Stolon, the algorithm is dead simple: just ask the coordinator about xact's
// status. If it is unavailable, wait until Stolon restores it.
// To save some trees, we try to keep connections persistent. Each rg is served
// by its own goroutine, and main goroutine coordinates them.
// Bidirectional communication is subject to deadlocks. To avoid them,
// -- main goroutine *never* blocks: before sending something, it makes sure
//    there is a slot in the chan (it is safe since nobody but it writes to worker
//    chans). If there is no slot, it silently drops the message -- worker will
//    later repeat its request to resolve xact.
// -- main goroutine must exit after all workers, or live worker might hang in
//    sending stuff to it. Simple atomic counter enforces that.
// gid format pgfdw:$timestamp:$sys_id:$pid:$xid:$participants_count:$coord_count is assumed
func xactResolverMain(ctx context.Context, clstatechan chan clusterState, wg *sync.WaitGroup) {
	defer wg.Done()
	var state = xactResolverState{
		xrworkers:        make(map[int]chan interface{}),
		resolve_requests: make(map[string][]int),
		in:               make(chan interface{}),
		retryBcstClstateTimer: time.NewTimer(0),
	}
	var workers_count int32 = 0 // atomic, used for correct shutdown
	<-state.retryBcstClstateTimer.C
	xrm_log("starting")

MainLoop:
	for {
		select {
		case <-ctx.Done():
			// some workers might be hanging sending us something,
			// don't exit until all of them are done
			if atomic.LoadInt32(&workers_count) != 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			xrm_log("stopped")
			return

		case state.clstate = <-clstatechan:
			// shut down workers for removed repgroups
			for rgid, ch := range state.xrworkers {
				if _, ok := state.clstate.rgs[rgid]; !ok {
					close(ch)
					delete(state.xrworkers, rgid)
				}
			}
			// spin up workers for new repgroups
			for rgid, _ := range state.clstate.rgs {
				if _, ok := state.xrworkers[rgid]; !ok {
					state.xrworkers[rgid] = make(chan interface{}, xactResolverWorkerChanBuf)
					atomic.AddInt32(&workers_count, 1)
					go xactResolverWorkerMain(ctx, rgid, state.xrworkers[rgid], state.in, state.clstate, &workers_count)
				}
			}
			xrBroadcastClusterData(&state)

		case <-state.retryBcstClstateTimer.C:
			xrBroadcastClusterData(&state)

		case msg := <-state.in:
			switch msg := msg.(type) {
			case xResolveRequest:
				if requesters, ok := state.resolve_requests[msg.gid]; ok {
					// we already know about that request
					// and sent the inquiry; just remember
					// that this rgid is also interested in
					// result, if not yet
					for _, r := range requesters {
						if r == msg.requester {
							continue MainLoop
						}
					}
					state.resolve_requests[msg.gid] = append(requesters, msg.requester)
					continue
				}
				// ok, try to inquiry this
				gid_splitted := strings.Split(msg.gid, ":")
				if len(gid_splitted) < 7 {
					xrm_log("format of gid %v from rg %d is wrong, ignoring it", msg.gid, msg.requester)
					continue
				}
				coord_sysid, err := strconv.ParseInt(gid_splitted[2], 10, 64)
				if err != nil {
					xrm_log("couldn't parse sysid of gid %v, ignoring it", msg.gid)
					continue
				}
				for rgid, rg := range state.clstate.rgs {
					if rg.sysId == coord_sysid {
						// found the coordinator
						ch := state.xrworkers[rgid]
						if len(ch) < xactResolverWorkerChanBuf {
							ch <- xStatusRequest{gid: msg.gid}
							// remember who was asking
							state.resolve_requests[msg.gid] = []int{msg.requester}
						}
						continue MainLoop
					}
				}
				xrm_log("ERROR: failed to resolve %s xact from repgroup %d: there is no rg with coordinator sysid %d in the cluster",
					msg.gid, msg.requester, coord_sysid)

			case xStatusResponse:
				xrm_log("DEBUG: status of xact %v is %v", msg.gid, msg.xStatus)
				if msg.xStatus == xStatusUnknown {
					xrm_log("ERROR: xact %s is too old to resolve it (status on coordinator is unknown)", msg.gid)
				}
				if msg.xStatus == xStatusCommitted ||
					msg.xStatus == xStatusAborted {
					for _, requester := range state.resolve_requests[msg.gid] {
						// unlikely, but requester might be gone
						if ch, ok := state.xrworkers[requester]; ok {
							if len(ch) < xactResolverWorkerChanBuf {
								ch <- xResolveResponse{gid: msg.gid, xStatus: msg.xStatus}
							}
						}
					}
				}
				delete(state.resolve_requests, msg.gid)
			}
		}
	}
}

func xrBroadcastClusterData(state *xactResolverState) {
	var try_again = false
	// Send only if we are sure we won''t block; otherwise remind
	// ourselves to try again later. Don't bother remembering who
	// exactly couldn't accept it previous time, send to all.
	// In case you wonder, single quote is funnily doubled to fix go-mode.el
	// parser.
	for _, ch := range state.xrworkers {
		if len(ch) < xactResolverWorkerChanBuf {
			ch <- state.clstate
		} else {
			try_again = true
		}
	}
	if try_again {
		state.retryBcstClstateTimer = time.NewTimer(retryBcstClstateInterval)
	} else {
		state.retryBcstClstateTimer = time.NewTimer(0)
		<-state.retryBcstClstateTimer.C
	}
}

func xrw_log(rgid int, format string, a ...interface{}) {
	args := []interface{}{rgid}
	args = append(args, a...)
	log.Printf("Xact resolver %d: "+format, args...)
}

const checkPreparesInterval = 5 * time.Second

// We only try to resolve prepares created at least resolvePrepareTimeout seconds ago
const resolvePrepareTimeout = 5

func xactResolverWorkerMain(ctx context.Context, rgid int, in <-chan interface{}, out chan<- interface{}, clstate clusterState, pworkers_count *int32) {
	// if conn is nil, we must wait until retryConnTimer fixes that
	var conn *pgx.Conn = nil
	var connstr string = pg.ConnString(clstate.rgs[rgid].connstrmap)
	var retryConnTimer *time.Timer = time.NewTimer(0)
	checkPreparesTicker := time.NewTicker(checkPreparesInterval)
	defer checkPreparesTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
			atomic.AddInt32(pworkers_count, -1)
			xrw_log(rgid, "stopped")
			return

		case <-retryConnTimer.C:
			if conn != nil {
				panic("not nil conn in retryConnTimer.C")
			}
			connconfig, err := pgx.ParseConnectionString(connstr)
			if err != nil {
				xrw_log(rgid, "failed to parse connstr %s: %v", connstr, err)
				// no point to retry until new connstr arrives
				continue
			}
			conn, err = pgx.Connect(connconfig)
			if err != nil {
				xrw_log(rgid, "unable to connect to database: %v", err)
				retryConnTimer = time.NewTimer(retryConnInterval)
			}

		case msg, ok := <-in:
			if !ok {
				if conn != nil {
					conn.Close()
				}
				atomic.AddInt32(pworkers_count, -1)
				xrw_log(rgid, "exit")
				return
			}
			switch msg := msg.(type) {
			case clusterState:
				newconnstr := pg.ConnString(clstate.rgs[rgid].connstrmap)
				// if connstr changed, invalidate connection
				if conn != nil && newconnstr != connstr {
					conn.Close()
					conn = nil
					retryConnTimer = time.NewTimer(0)
				}
				connstr = newconnstr

			case xStatusRequest:
				if conn == nil {
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusFailedToCheck}
					continue
				}
				gid_splitted := strings.Split(msg.gid, ":")
				if len(gid_splitted) < 7 {
					xrw_log(rgid, "format of gid %v is wrong, ignoring it", msg.gid)
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusFailedToCheck}
					continue
				}
				xid := gid_splitted[4]
				var statusp *string
				err := conn.QueryRow(fmt.Sprintf(
					"select txid_status(%s)", xid)).Scan(&statusp)
				if err != nil {
					xrw_log(rgid, "failed to check xact %s (xid %s) status: %v", msg.gid, xid, err)
					xrw_connfail(&conn, &retryConnTimer)
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusFailedToCheck}
					continue
				}
				if statusp == nil {
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusUnknown}
				} else if *statusp == "committed" {
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusCommitted}
				} else if *statusp == "aborted" {
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusAborted}
				} else {
					if *statusp != "in progress" {
						panic(fmt.Sprintf("wrong xact status: %v", *statusp))
					}
					out <- xStatusResponse{gid: msg.gid, xStatus: xStatusInProgress}
				}

			case xResolveResponse:
				if conn == nil {
					// conn not ready, handle it later, drop for now
					continue
				}
				if msg.xStatus != xStatusCommitted && msg.xStatus != xStatusAborted {
					panic(fmt.Sprintf("only useful outcomes must reach xr worker, got %v for %v",
						msg.xStatus, msg.gid))
				}
				var action string
				if msg.xStatus == xStatusCommitted {
					action = "commit"
				} else {
					action = "rollback"
				}
				_, err := conn.Exec(fmt.Sprintf("%s prepared %s",
					action, pg.QL(msg.gid)))
				if err != nil {
					xrw_log(rgid, "failed to finish (%v) xact %v: %v",
						action, msg.gid, err)
					// it might be not conn error, but simpler to reconnect anyway
					xrw_connfail(&conn, &retryConnTimer)
				}
			}

		// TODO: get notified about new prepares via NOTIFY?
		case <-checkPreparesTicker.C:
			// xrw_log(rgid, "DEBUG: checking prepares")
			if conn == nil {
				continue
			}
			var gid string
			rows, err := conn.Query(fmt.Sprintf(
				"select gid from pg_prepared_xacts where extract(epoch from (current_timestamp - prepared))::int >= %d",
				resolvePrepareTimeout))
			if err != nil {
				xrw_log(rgid, "failed to retrieve prepares: %v", err)
				goto CheckPreparesErr
			}
			for rows.Next() {
				err = rows.Scan(&gid)
				if err != nil {
					xrw_log(rgid, "%v", err)
					goto CheckPreparesErr // xxx should be panic actually?
				}
				out <- xResolveRequest{requester: rgid, gid: gid}
			}
			if rows.Err() != nil {
				xrw_log(rgid, "%v", err)
				goto CheckPreparesErr // xxx should be panic actually?
			}
			continue
		CheckPreparesErr:
			xrw_connfail(&conn, &retryConnTimer)
		}
	}
}

func xrw_connfail(connp **pgx.Conn, retryConnTimerp **time.Timer) {
	(*connp).Close()
	*connp = nil
	*retryConnTimerp = time.NewTimer(0)
}

func dd_log(format string, a ...interface{}) {
	log.Printf("Deadlock detector main: "+format, a...)
}

type collectGraph struct{}

// a process holding/waiting lock
type proc struct {
	sysid int64
	pid   int
}
type edge struct {
	wait proc
	hold proc
}

// lock graph as delivered from each node
type localLockGraph struct {
	rgid  int
	err   error
	edges []edge
}
type cancelBackend struct {
	pid int
}
type cancelBackendResponse struct {
	err error
	res bool
}

// debug
func rgidBySysid(sysid int64, rgs map[int]*repGroupState) int {
	for rgid, rg := range rgs {
		if rg.sysId == sysid {
			return rgid
		}
	}
	return -1
}
func pprintEdges(edges []edge, rgs map[int]*repGroupState) string {
	var res = ""
	for _, e := range edges {
		res = res + fmt.Sprintf("%d:%d->%d:%d\n",
			rgidBySysid(e.wait.sysid, rgs), e.wait.pid,
			rgidBySysid(e.hold.sysid, rgs), e.hold.pid)
	}
	return res
}
func pprintLockGraph(lock_graph map[proc][]proc, rgs map[int]*repGroupState) string {
	var res = ""
	for p, edges := range lock_graph {
		res = res + fmt.Sprintf("%d:%d:\n",
			rgidBySysid(p.sysid, rgs), p.pid)
		for _, e := range edges {
			res = res + fmt.Sprintf("  %d:%d\n", rgidBySysid(e.sysid, rgs), e.pid)
		}
	}
	return res
}
func pprintDeadlock(deadlock []proc, rgs map[int]*repGroupState) string {
	var res = ""
	for _, p := range deadlock {
		res = res + fmt.Sprintf("%d:%d->", rgidBySysid(p.sysid, rgs), p.pid)
	}
	return res
}

// Deadlock detector. Again, to save CPU we try to keep connections persistent.
// In hope to increase probability of consistent picture we collect graphs in
// parallel, i.e. again each repgroup is served by its own goroutine. However,
// the communcation is pretty simple as it is entirely synchronous. We just need
// to make sure workers exit *after* main starts shutdown, or main might hang
// sending stuff to them.
// Because we can not make consistent distributed snapshot, collected global
// local graph can contain "false" loops.  So we report deadlock only if
// detected loop persists during deadlock detection period.
func deadlockDetectorMain(ctx context.Context, clstatechan chan clusterState, wg *sync.WaitGroup) {
	defer wg.Done()
	checkDeadlockTicker := time.NewTicker(checkDeadlockInterval)
	defer checkDeadlockTicker.Stop()

	var ddworkers = make(map[int]chan interface{})
	var in = make(chan interface{})
	var clstate clusterState
	var ddWg sync.WaitGroup
	var previousDeadlock []proc = nil

	dd_log("starting")

	for {
		select {
		case <-ctx.Done():
			for _, ch := range ddworkers {
				close(ch)
			}
			ddWg.Wait()
			dd_log("stopped")
			return

		case clstate = <-clstatechan:
			// shut down workers for removed repgroups
			for rgid, ch := range ddworkers {
				if _, ok := clstate.rgs[rgid]; !ok {
					previousDeadlock = nil
					close(ch)
					delete(ddworkers, rgid)
				}
			}
			// spin up workers for new repgroups
			for rgid, _ := range clstate.rgs {
				if _, ok := ddworkers[rgid]; !ok {
					ddWg.Add(1)
					ddworkers[rgid] = make(chan interface{})
					go deadlockDetectorWorker(rgid, ddworkers[rgid], in, clstate, &ddWg)
				}
			}

		case <-checkDeadlockTicker.C:
			for _, ch := range ddworkers {
				ch <- collectGraph{}
			}
			var fail = false
			// for each vertex, all outbound edges
			var lockGraph = make(map[proc][]proc)
			for i := 0; i < len(ddworkers); i++ {
				llg := (<-in).(localLockGraph)
				if llg.err != nil {
					dd_log("failed to collect lock graph at repgroup %d: %v", llg.rgid, llg.err)
					fail = true
					continue
				}
				for _, e := range llg.edges {
					if _, ok := lockGraph[e.wait]; !ok {
						lockGraph[e.wait] = []proc{e.hold}
					} else {
						lockGraph[e.wait] = append(lockGraph[e.wait], e.hold)
					}
					// *all* procs must be in lockGraph, even if
					// they don't wait for anything themselves
					if _, ok := lockGraph[e.hold]; !ok {
						lockGraph[e.hold] = []proc{}
					}
				}
				// dd_log("collected lg from %d:\n%v", llg.rgid, pprintEdges(llg.edges, clstate.rgs))
			}
			if fail {
				// loops collected with failure between them are unreliable
				previousDeadlock = nil
				continue
			}
			dd_log("DEBUG: full graph is\n%v", pprintLockGraph(lockGraph, clstate.rgs))
			deadlock := findDeadlock(lockGraph)
			if deadlock != nil {
				dd_log("DEBUG: found deadlock!\n  %v\n", pprintDeadlock(deadlock, clstate.rgs))
				dd_log("DEBUG: prev deadlock:\n  %v\n", pprintDeadlock(previousDeadlock, clstate.rgs))
				if deadlocksEquivalent(deadlock, previousDeadlock) {
					// time to kill someone
					victim_idx := rand.Intn(len(deadlock))
					victim := deadlock[victim_idx]
					victim_rgid := rgidBySysid(victim.sysid, clstate.rgs)
					dd_log("INFO: Deadlock discovered:\n  %v\n  Canceling pid %d at repgroup %d...",
						pprintDeadlock(deadlock, clstate.rgs), victim.pid, victim_rgid)
					ddworkers[victim_rgid] <- cancelBackend{pid: victim.pid}
					tbr := (<-in).(cancelBackendResponse)
					if tbr.err != nil {
						dd_log("INFO: failed to cancel backend %d at repgroup %d: %v",
							victim.pid, victim_rgid, tbr.err)
					} else if !tbr.res {
						dd_log("INFO: Canceling backend %d at repgroup %d missed: pg_cancel_backend returned false",
							victim.pid, victim_rgid)
					} else {
						dd_log("INFO: successfully canceled backend %d at repgroup %d",
							victim.pid, victim_rgid)
					}
				}
			}
			previousDeadlock = deadlock

		case msg := <-in:
			switch msg := msg.(type) {
			case localLockGraph:
				log.Printf("%v", msg)
			}
		}
	}
}

func deadlockDetectorWorker(rgid int, in <-chan interface{}, out chan<- interface{}, clstate clusterState, wg *sync.WaitGroup) {
	var conn *pgx.Conn = nil
	var connstr string = pg.ConnString(clstate.rgs[rgid].connstrmap)

	for {
		msg, ok := <-in
		if !ok {
			if conn != nil {
				conn.Close()
			}
			wg.Done()
			return

		}
		switch msg := msg.(type) {
		case clusterState:
			newconnstr := pg.ConnString(msg.rgs[rgid].connstrmap)
			// if connstr changed, invalidate connection
			if conn != nil && newconnstr != connstr {
				conn.Close()
				conn = nil
			}

		case collectGraph:
			if conn == nil {
				connconfig, err := pgx.ParseConnectionString(connstr)
				if err != nil {
					out <- localLockGraph{rgid: rgid, err: err}
					continue
				}
				conn, err = pgx.Connect(connconfig)
				if err != nil {
					out <- localLockGraph{rgid: rgid, err: err}
					continue
				}
			}
			var err error
			var rows *pgx.Rows
			var edges = make([]edge, 0, 128)
			rows, err = conn.Query("select * from hodgepodge.lock_graph_native_types")
			if err != nil {
				goto ConnError
			}
			for rows.Next() {
				var e edge
				err = rows.Scan(&e.wait.sysid, &e.wait.pid, &e.hold.sysid, &e.hold.pid)
				if err != nil {
					goto ConnError // xxx panic here?
				}
				edges = append(edges, e)
			}
			if rows.Err() != nil {
				goto ConnError
			}
			out <- localLockGraph{rgid: rgid, err: nil, edges: edges}
			continue
		ConnError:
			conn.Close()
			conn = nil
			out <- localLockGraph{rgid: rgid, err: err}

		case cancelBackend:
			// conn *must* be not nil here; we terminate iff
			// collected graph successfully
			var res bool
			err := conn.QueryRow(fmt.Sprintf("select pg_cancel_backend(%d)",
				msg.pid)).Scan(&res)
			if err != nil {
				conn.Close()
				conn = nil
			}
			out <- cancelBackendResponse{res: res, err: err}
		}
	}
}

// wrapper of []proc for findDeadlock to remember visited vertices
type vertex struct {
	e       []proc // outbound edges
	visited bool
	// set to true when we have checked all paths from vertex and ensured
	// there is no loop
	no_deadlocks bool
}

// Actually find the loop. Returns nil if it doesn't exist, random one otherwise.
func findDeadlock(lockGraph map[proc][]proc) []proc {
	// wrap []proc edges into struct vertex to remember visited vertices
	var lockGraphV = make(map[proc]*vertex)
	for p, edges := range lockGraph {
		lockGraphV[p] = &vertex{e: edges, visited: false, no_deadlocks: false}
	}
	// For each vertice v, start dfs from v looking for deadlocks
	for p, _ := range lockGraphV {
		deadlock := findDeadlockWorkhorse(lockGraphV, nil, p)
		if deadlock != nil {
			// Returned path might contain not only the loop itself,
			// but also initial tail. Trim it here. We also cut copy
			// of doubled vertex, it is *not* included in the
			// final res: deadlock[len(deadlock) - 1] -> deadlock[0]
			// link is assumed.
			var deadlock_member proc = deadlock[len(deadlock)-1]
			for i, p := range deadlock {
				if p == deadlock_member {
					return deadlock[i : len(deadlock)-1]
				}
			}
			return deadlock
		}
	}
	return nil
}

// one step: with 'path' passed, visit p and its childs. If deadlock if found,
// returns it; the last vertex closures the loop. Otherwise returns nil.
func findDeadlockWorkhorse(lockGraph map[proc]*vertex, path []proc, p proc) []proc {
	var vp *vertex = lockGraph[p]
	// return immediatly, if we have checked p before
	if vp.no_deadlocks {
		return nil
	}
	path = append(path, p)
	if vp.visited {
		return path // found!
	}
	// ok, mark that we were here
	vp.visited = true
	// and let's go deeper
	for _, child := range vp.e {
		deadlock := findDeadlockWorkhorse(lockGraph, path, child)
		if deadlock != nil {
			return deadlock
		}
	}
	// not found; remember there is nothing to do here
	vp.no_deadlocks = true
	// And note that this vertex is not in path anymore. Actually, this is not
	// necessary as we use 'no_deadlocks' optimization: if already seen vertex
	// doesn't belong to any loop, we ping back from it immediately
	vp.visited = false
	return nil
}

// deadlocks collected at different times might be shifted
func deadlocksEquivalent(d1 []proc, d2 []proc) bool {
	if len(d1) != len(d2) {
		return false
	}
	var start_p proc = d1[0]
	var i2 int = 0
	for i, p := range d2 {
		if p == start_p {
			i2 = i
		}
	}
	for _, p1 := range d1 {
		if p1 != d2[i2] {
			return false
		}
		i2 = (i2 + 1) % len(d1)
	}
	return true
}
