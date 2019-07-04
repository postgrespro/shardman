// Copyright (c) 2018, Postgres Professional

// Interaction with Postgres. Partly from Stolon based on github.com/lib/pq, ha
package pg

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx"

	"postgrespro.ru/shardman/internal/cluster"
)

// Broadcaster

type Broadcaster struct {
	conns    map[int]*BroadcastConn
	resch    chan resT
	reportch chan report
}

type BroadcastConn struct {
	in      chan interface{}
	connstr string
}

type report struct {
	err error // nil ~ ok
	id  int
}

type resT struct {
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

func broadcastConnMain(in <-chan interface{}, resch chan<- resT, reportch chan<- report, connstr string, myid int) {
	var report = report{err: nil, id: myid}
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
				resch <- resT{res: *res, id: myid}
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
				resch <- resT{res: *res, id: myid}
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
					report.err = fmt.Errorf("sql \n%v\n failed: %v", sql, err)
				}

				// TODO currently assuming queries return single text attr or nothing
				for rows.Next() {
					err = rows.Scan(&res)
					if err != nil {
						report.err = fmt.Errorf("scan sql \n%v\n failed: %v", sql, err)
					}
				}
				if rows.Err() != nil {
					report.err = fmt.Errorf("sql \n%v\n failed: %v", sql, rows.Err())
				}
				rows.Close()
			}
		}
	}
}

func NewBroadcaster(cs *cluster.ClusterStore, rgs map[int]*cluster.RepGroup, cldata *cluster.ClusterData) (*Broadcaster, error) {
	var bcst = Broadcaster{conns: map[int]*BroadcastConn{}}

	// learn connstrs
	for rgid, rg := range rgs {
		connstr, err := GetSuConnstr(context.TODO(), cs, rg, cldata)
		if err != nil {
			return nil, err
		}
		bcst.conns[rgid] = &BroadcastConn{
			in:      make(chan interface{}),
			connstr: connstr,
		}
	}

	bcst.resch = make(chan resT)
	bcst.reportch = make(chan report)

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

func (bcst *Broadcaster) PushAll(sql string) {
	for _, bconn := range bcst.conns {
		bconn.in <- sql
	}
}

func (bcst *Broadcaster) Commit(twophase bool) (map[int]string, error) {
	var results = map[int]string{}
	if twophase {
		for _, bconn := range bcst.conns {
			bconn.in <- Prepare{gid: "shardman"}
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
				bconn.in <- CommitPrepared{gid: "shardman"}
			} else {
				bconn.in <- RollbackPrepared{gid: "shardman"}
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

// get connstr of each repgroup
func GetSuConnstrs(ctx context.Context, cs *cluster.ClusterStore) (map[int]string, error) {
	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return nil, err
	}
	if cldata == nil {
		return nil, fmt.Errorf("cluster data not found")
	}
	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("Failed to get repgroups: %v", err)
	}

	var connstrs = make(map[int]string)
	for rgid, rg := range rgs {
		connstr, err := GetSuConnstr(ctx, cs, rg, cldata)
		if err != nil {
			return nil, err
		}
		connstrs[rgid] = connstr
	}
	return connstrs, nil
}

// Get connstring + priority of current master
func GetSuConnstrWithPriority(ctx context.Context, cs *cluster.ClusterStore, rg *cluster.RepGroup, cldata *cluster.ClusterData) (string, int, error) {
	// always fetch single endpoint, because the only callers are Go code,
	// and pgx doesn't support multiple hosts.
	cp, priority, err := cs.GetSuConnstrMap(ctx, rg, cldata, true)
	if err != nil {
		return "", 0, err
	}
	return ConnString(cp), priority, nil
}

// just su connstring
func GetSuConnstr(ctx context.Context, cs *cluster.ClusterStore, rg *cluster.RepGroup, cldata *cluster.ClusterData) (string, error) {
	// always fetch single endpoint, because the only callers are Go code,
	// and pgx doesn't support multiple hosts.
	connstr, _, err := GetSuConnstrWithPriority(ctx, cs, rg, cldata)
	return connstr, err
}

// ConnString returns a connection string, its entries are sorted so the
// returned string can be reproducible and comparable
func ConnString(p map[string]string) string {
	var kvs []string
	escaper := strings.NewReplacer(`'`, `\'`, `\`, `\\`)
	for k, v := range p {
		if v != "" {
			var val string = fmt.Sprintf("%s", escaper.Replace(v))
			if k == "port" {
				val = v /* pgx complains on port='5432' */
			}
			kvs = append(kvs, fmt.Sprintf("%s=%s", k, val))
		}
	}
	sort.Sort(sort.StringSlice(kvs))
	return strings.Join(kvs, " ")
}

// postgres_fdw accepts user/password params in user mapping opts and
// everything else in foreign server ones...
func FormUserMappingOpts(p map[string]string) (string, error) {
	if _, ok := p["user"]; !ok {
		return "", fmt.Errorf("username not specified")
	}
	res := fmt.Sprintf("options (user %s", QL(p["user"]))
	if password, ok := p["password"]; ok {
		res = fmt.Sprintf("%s, password %s", res, QL(password))
	}
	return fmt.Sprintf("%s)", res), nil
}
func FormForeignServerOpts(p map[string]string) (string, error) {
	if _, ok := p["dbname"]; !ok {
		return "", fmt.Errorf("dbname not specified")
	}
	if _, ok := p["host"]; !ok {
		return "", fmt.Errorf("host not specified")
	}
	if _, ok := p["port"]; !ok {
		return "", fmt.Errorf("port not specified")
	}

	res := fmt.Sprintf("options (dbname %s, host %s, port '%s')",
		QL(p["dbname"]),
		QL(p["host"]),
		p["port"])
	return res, nil
}

// PG's quote_identifier. FIXME keywords
func QI(ident string) string {
	var safe = true
	for _, r := range ident {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r == '_')) {
			safe = false
			break
		}
	}
	if safe {
		return ident
	}
	var sql_ident_escaper = strings.NewReplacer(`"`, `""`)
	return fmt.Sprintf("\"%s\"", sql_ident_escaper.Replace(ident))
}

// PG's quote literal
func QL(lit string) string {
	var sql_str_escaper = strings.NewReplacer(`'`, `''`)
	return fmt.Sprintf("'%s'", sql_str_escaper.Replace(lit))
}

func P(relname string, pnum int) string {
	return fmt.Sprintf("%s_%d", relname, pnum)
}

// "I" is identifier, "L" is literal
func PI(relname string, pnum int) string {
	return QI(P(relname, pnum))
}
func PL(relname string, pnum int) string {
	return QL(P(relname, pnum))
}
func FP(relname string, pnum int) string {
	return fmt.Sprintf("%s_%d_fdw", relname, pnum)
}
func FPI(relname string, pnum int) string {
	return QI(FP(relname, pnum))
}
func FPL(relname string, pnum int) string {
	return QL(FP(relname, pnum))
}
func FSI(rgid int) string {
	return fmt.Sprintf("hp_rg_%d", rgid)
}
func FSL(rgid int) string {
	return fmt.Sprintf("'hp_rg_%d'", rgid)
}

// Tables are not actually stored in the store
func GetTables(cs *cluster.ClusterStore, ctx context.Context) ([]cluster.Table, error) {
	var tables []cluster.Table

	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return nil, err
	}
	rgs, _, err := cs.GetRepGroups(ctx)
	if err != nil {
		return nil, err
	}
	var connstr string
	if len(rgs) == 0 {
		return nil, fmt.Errorf("can't get tables, no repgroups in cluster")
	}
	for _, rg := range rgs {
		connstr, err = GetSuConnstr(ctx, cs, rg, cldata)
		if err != nil {
			return nil, err
		}
		break
	}

	connconfig, err := pgx.ParseConnectionString(connstr)
	conn, err := pgx.Connect(connconfig)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to database: %v", err)
	}
	defer conn.Close()

	rows, err := conn.Query(
		`select n.nspname, c.relname, nparts, cn.nspname colocated_nspname, cc.relname colocated_relname from shardman.sharded_tables s join pg_class c on (s.rel = c.oid) join
pg_namespace n on (c.relnamespace = n.oid) left outer join
pg_class cc on (cc.oid = s.colocated_with) left outer join
pg_namespace cn on (cn.oid = cc.relnamespace);`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var t cluster.Table
		var colocateWithSchema *string
		var colocateWithRelname *string
		err = rows.Scan(&t.Schema, &t.Relname, &t.Nparts, &colocateWithSchema, &colocateWithRelname)
		if colocateWithSchema != nil {
			t.ColocateWithSchema = *colocateWithSchema
			t.ColocateWithRelname = *colocateWithRelname
		}
		if err != nil {
			return nil, err
		}
		t.Partmap = make([]int, t.Nparts)
		tables = append(tables, t)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// fill partmaps
	for i, t := range tables {
		rows, err := conn.Query(fmt.Sprintf("select pnum, rgid from shardman.parts where rel = %s::regclass",
			QL(fmt.Sprintf("%s.%s", QI(t.Schema), QI(t.Relname)))))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var pnum, rgid int
			err = rows.Scan(&pnum, &rgid)
			if err != nil {
				return nil, err
			}
			tables[i].Partmap[pnum] = rgid
		}
		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}
	return tables, nil
}
