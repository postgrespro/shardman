// Interaction with Postgres. Partly from Stolon based on github.com/lib/pq, ha
package pg

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/store"
)

// Form super connstr for this master
func FormSuConnstr(master *cluster.Master, cldata *cluster.ClusterData) string {
	cp := map[string]string{
		"user":   cldata.PgSuUsername,
		"dbname": "postgres",
		"host":   master.ListenAddress,
		"port":   master.Port,
	}
	if cldata.PgSuAuthMethod != "trust" {
		cp["password"] = cldata.PgSuPassword
	}
	return ConnString(cp)
}

// Broadcaster

type Broadcaster struct {
	conns    map[int]*BroadcastConn
	resch    chan Res
	reportch chan Report
}

type BroadcastConn struct {
	in      chan interface{}
	connstr string
}

type Report struct {
	err error // nil ~ ok
	id  int
}

type Res struct {
	res string
	id  int
}

type Begin struct{}
type Commit struct{}
type Prepare struct {
	gid string
}
type CommitPrepared struct {
	gid string
}
type RollbackPrepared struct {
	gid string
}

func broadcastConnMain(in <-chan interface{}, resch chan<- Res, reportch chan<- Report, connstr string, myid int) {
	var report = Report{err: nil, id: myid}
	var res *string = nil
	var tx *pgx.Tx = nil
	var prepare_exists = false

	connconfig, err := pgx.ParseConnectionString(connstr)
	conn, err := pgx.Connect(connconfig)
	if err != nil {
		report.err = fmt.Errorf("Unable to connect to database: %v", err)
	} else {
		defer conn.Close()
	}
	reportch <- report
	report.err = nil

	// reporting is allowed (and required) only after certain messages
	for msg := range in {
		switch msg := msg.(type) {
		case Begin:
			if report.err == nil {
				tx, err = conn.Begin()
				if err != nil {
					report.err = fmt.Errorf("BEGIN failed: %v", err)
				}
			}
		case Commit:
			// Commit if everything is ok; otherwise just report last error
			if report.err == nil {
				err = tx.Commit()
				if err != nil {
					report.err = fmt.Errorf("COMMIT failed: %v", err)
				}
			}
			if report.err == nil && res != nil {
				resch <- Res{res: *res, id: myid}
			}
			reportch <- report
			report.err = nil
		case Prepare:
			if report.err == nil {
				_, err = conn.Exec(fmt.Sprintf("prepare transaction '%s'", msg.gid))
				if err != nil {
					report.err = fmt.Errorf("PREPARE failed: %v", err)
				}
				prepare_exists = true
			}
			if report.err == nil && res != nil {
				resch <- Res{res: *res, id: myid}
			}
			reportch <- report
			report.err = nil
		case RollbackPrepared:
			if prepare_exists {
				_, err = conn.Exec(fmt.Sprintf("rollback prepared '%s'", msg.gid))
				if err != nil {
					report.err = fmt.Errorf("ROLLBACK PREPARED failed: %v", err)
				}
				reportch <- report
				report.err = nil
				prepare_exists = false
			} else {
				// No prepare means we reported error before
				// Send empty report since answer is awaited anyway
				reportch <- report
			}
		case CommitPrepared:
			if prepare_exists {
				_, err = conn.Exec(fmt.Sprintf("commit prepared '%s'", msg.gid))
				if err != nil {
					report.err = fmt.Errorf("COMMIT PREPARED failed: %v", err)
				}
				reportch <- report
				report.err = nil
				prepare_exists = false
			} else {
				// No prepare means we reported error before
				// Send empty report since answer is awaited anyway
				reportch <- report
			}
		case string:
			sql := msg
			// Run always Query instead of bookkeeping whether we should
			// Exec or Query. Testing shows it works.
			if report.err == nil {
				rows, err := conn.Query(sql)
				if err != nil {
					report.err = fmt.Errorf("sql %v failed: %v", sql, err)
				}

				// TODO Assume queries return single text attr or nothing
				for rows.Next() {
					err = rows.Scan(&res)
					if err != nil {
						report.err = fmt.Errorf("scan sql %v failed: %v", sql, err)
					}
				}
				if rows.Err() != nil {
					report.err = fmt.Errorf("sql %v failed: %v", sql, rows.Err())
				}
				rows.Close()
			}
		}
	}
}

func NewBroadcaster(rgs map[int]*cluster.RepGroup, cldata *cluster.ClusterData) (*Broadcaster, error) {
	var bcst = Broadcaster{conns: map[int]*BroadcastConn{}}

	// learn connstrs
	for rgid, rg := range rgs {
		ss, err := store.NewStolonStore(rg)
		if err != nil {
			return nil, err
		}
		defer ss.Close()

		master, err := ss.GetMaster(context.TODO())
		if err != nil {
			return nil, err
		}
		bcst.conns[rgid] = &BroadcastConn{
			in:      make(chan interface{}),
			connstr: FormSuConnstr(master, cldata),
		}
	}

	bcst.resch = make(chan Res)
	bcst.reportch = make(chan Report)

	for id, bconn := range bcst.conns {
		go broadcastConnMain(bconn.in, bcst.resch, bcst.reportch, bconn.connstr, id)
	}
	// Check connection success
	var err error = nil
	for i := 0; i < len(bcst.conns); i++ {
		report := <-bcst.reportch
		if report.err != nil {
			errmsg := fmt.Sprintf("Repgroup %v failed: %v\n",
				report.id, report.err)
			if err == nil {
				err = fmt.Errorf(errmsg)
			} else {
				err = fmt.Errorf("%v%v", err, report.err)
			}
		}
	}
	if err != nil {
		bcst.Close()
		return nil, err
	}

	return &bcst, nil
}

func (bcst *Broadcaster) Begin() {
	for _, bconn := range bcst.conns {
		bconn.in <- Begin{}
	}
}

func (bcst *Broadcaster) Push(id int, sql string) {
	bcst.conns[id].in <- sql
}

func (bcst *Broadcaster) Commit(twophase bool) (map[int]string, error) {
	var results = map[int]string{}
	if twophase {
		for _, bconn := range bcst.conns {
			bconn.in <- Prepare{gid: "hodgepodge"}
		}
	} else {
		for _, bconn := range bcst.conns {
			bconn.in <- Commit{}
		}
	}
	var err error = nil
	var nreports int = 0
	for nreports < len(bcst.conns) {
		select {
		case report := <-bcst.reportch:
			if report.err != nil {
				errmsg := fmt.Sprintf("Repgroup %v failed: %v\n",
					report.id, report.err)
				if err == nil {
					err = fmt.Errorf(errmsg)
				} else {
					err = fmt.Errorf("%v%v", err, report.err)
				}
			}
			nreports++
		case res := <-bcst.resch:
			results[res.id] = res.res
		}
	}
	if twophase {
		for _, bconn := range bcst.conns {
			if err == nil {
				bconn.in <- CommitPrepared{gid: "hodgepodge"}
			} else {
				bconn.in <- RollbackPrepared{gid: "hodgepodge"}
			}
		}
		for nreports = 0; nreports < len(bcst.conns); nreports++ {
			report := <-bcst.reportch
			if report.err != nil {
				errmsg := fmt.Sprintf("Repgroup %v failed: %v\n",
					report.id, report.err)
				if err == nil {
					err = fmt.Errorf(errmsg)
				} else {
					err = fmt.Errorf("%v%v", err, report.err)
				}
			}
		}
	}
	return results, err
}

// Close channels to avoid goroutines leakage
func (bcst *Broadcaster) Close() {
	for _, bconn := range bcst.conns {
		close(bconn.in)
	}
}

// Connstring stuff

// ConnString returns a connection string, its entries are sorted so the
// returned string can be reproducible and comparable
func ConnString(p map[string]string) string {
	var kvs []string
	escaper := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	for k, v := range p {
		if v != "" {
			kvs = append(kvs, k+"="+escaper.Replace(v))
		}
	}
	sort.Sort(sort.StringSlice(kvs))
	return strings.Join(kvs, " ")
}
