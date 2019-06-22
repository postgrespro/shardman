// Copyright (c) 2018, Postgres Professional

package ladle

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/cluster/commands"
	"postgrespro.ru/shardman/internal/pg"
	"postgrespro.ru/shardman/internal/shmnlog"
	"postgrespro.ru/shardman/internal/store"
)

const (
	CurrentFormatVersion = 1
)

// stored under one key (clustername/ladledata) in store for simplicity
type LadleData struct {
	FormatVersion uint64
	Spec          LadleSpec
	// computed layout: which daemons are on which nodes (hostnames)
	Layout map[string]*NodeLayout
	// how many monitors are configured
	MonitorsNum int
	// this is kinda redundant as it can be recomputed from layout, but
	// probably makes distribution more visible
	Clovers map[int][]string
	// sequence for clover nums
	CloverSeq int
}

const (
	LadlePlacementPolicyClover  = "clover"
	LadlePlacementPolicySausage = "sausage"
)

// Configuration of the cluster defining ladle behaviour
type LadleSpec struct {
	// shardman monitor and stolon daemons need to know how to get to the
	// store; we keep this info here. For convenience, during init it is
	// copied from usual store conn arguments if not specified in spec file
	// directly
	StoreConnInfo *cluster.StoreConnInfo

	// how many replicas we want
	Repfactor       *int
	PlacementPolicy string

	// directory with all (mostly keepers) data
	DataDir string

	// path to stolon binaries; if empty, PATH is used
	StolonBinPath string

	// path to postgres binaries; if empty, PATH is used
	PgBinPath string
	// starting from which port to assign ports to keepers
	KeepersInitialPort int

	// how many instances of shardman-monitor to run
	MonitorsNum *int

	ProxyPort int
}

type NodeLayout struct {
	// for simplicity, we always put sentinel alongside the keeper
	Keepers []Keeper
	// monitor assigned to this node?
	Monitor bool
	// For simplicity and due to common sense we always put *single* proxy
	// to each node belonging to rg with 'preferred' keeper on this node
	// (this is a ptr as we might once decide to go without proxies)
	Proxy *Proxy
}

type Keeper struct {
	Id        KeeperId
	Preferred bool
	PgPort    int
}

type KeeperId struct {
	// Stolon instance this keeper belongs to
	RepGroup string
	// Number of keeper inside Stolon instance.
	// In general, keeper is fully identified by clustername-rgname-id
	Uid int
}

type Proxy struct {
	// Stolon instance this proxy belongs to
	RepGroup string
	Port     int
}

// Store is the same for ladle and cluster: it's just endpoint + cluster name,
// so inherit it.
type LadleStore struct {
	*cluster.ClusterStore
}

func NewLadleStore(cfg *cluster.ClusterStoreConnInfo) (*LadleStore, error) {
	cs, err := cluster.NewClusterStore(cfg)
	if err != nil {
		return nil, err
	}
	return &LadleStore{ClusterStore: cs}, nil
}

func (ls *LadleStore) LadleDataStorePath() string {
	return filepath.Join(ls.StorePath, "ladledata")
}

func (ls *LadleStore) GetLadleData(ctx context.Context) (*LadleData, *store.KVPair, error) {
	var ldata = &LadleData{}
	path := ls.LadleDataStorePath()
	pair, err := ls.Store.Get(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	if pair == nil {
		return nil, nil, nil
	}
	if err := json.Unmarshal(pair.Value, ldata); err != nil {
		return nil, nil, err
	}
	return ldata, pair, nil
}

func (ls *LadleStore) GetLadleAndClusterData(ctx context.Context) (*LadleData, *store.KVPair, *cluster.ClusterData, *store.KVPair, error) {
	cldata, clpair, err := ls.GetClusterData(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Error retrieving cluster data: %v", err)
	}
	ldata, lpair, err := ls.GetLadleData(ctx)
	if err != nil {
		return nil, nil, cldata, clpair, fmt.Errorf("Error retrieving ladle data: %v", err)
	}
	return ldata, lpair, cldata, clpair, nil
}

func (ls *LadleStore) PutLadleData(ctx context.Context, ldata *LadleData) error {
	ldataj, err := json.Marshal(ldata)
	if err != nil {
		return err
	}
	path := ls.LadleDataStorePath()
	return ls.Store.Put(ctx, path, ldataj)
}

// scribbles directly on input
func adjustSpecDefaults(spec *LadleSpec, clusterStoreConnInfo *cluster.StoreConnInfo) {
	if spec.StoreConnInfo == nil {
		// copy for extra safety
		c := *clusterStoreConnInfo
		spec.StoreConnInfo = &c
	}

	if spec.PlacementPolicy == "" {
		spec.PlacementPolicy = LadlePlacementPolicyClover
	}
	if spec.Repfactor == nil {
		defaultRepfactor := 1
		spec.Repfactor = &defaultRepfactor
	}

	if spec.KeepersInitialPort == 0 {
		spec.KeepersInitialPort = 5442
	}
	if spec.MonitorsNum == nil {
		defaultMonitorsNum := 2
		spec.MonitorsNum = &defaultMonitorsNum
	}
	if spec.ProxyPort == 0 {
		spec.ProxyPort = 5432
	}
}

func validateSpec(spec *LadleSpec) error {
	if spec.StoreConnInfo.Endpoints == "" {
		return fmt.Errorf("store endpoints not specified")
	}
	if spec.DataDir == "" {
		return fmt.Errorf("DataDir not specified")
	}
	if spec.PlacementPolicy != LadlePlacementPolicyClover {
		return fmt.Errorf("wrong placement policy: %v", spec.PlacementPolicy)
	}
	return nil
}

func (ls *LadleStore) InitCluster(ctx context.Context, hl *shmnlog.Logger, spec *LadleSpec, clusterSpec *cluster.ClusterSpec, clusterStoreConnInfo *cluster.ClusterStoreConnInfo) error {
	// fill defaults and validate config
	adjustSpecDefaults(spec, &clusterStoreConnInfo.StoreConnInfo)
	if err := validateSpec(spec); err != nil {
		return err
	}

	// First put cluster data, then ladle data to the cluster. Order is not
	// particularly important, but in any case this is not transactional and
	// in extremely unlucky cases only one of them might succeed; need to
	// retry then.
	err := commands.InitCluster(ctx, ls.ClusterStore, clusterSpec)
	if err != nil {
		return err
	}

	ldata, _, err := ls.GetLadleData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get ladle data: %v", err)
	}
	if ldata != nil {
		hl.Warnf("overriding existing ladle data")
	}
	ldatanew := &LadleData{
		FormatVersion: CurrentFormatVersion,
		Spec:          *spec,
		Layout:        make(map[string]*NodeLayout),
		MonitorsNum:   0,
		Clovers:       make(map[int][]string),
		CloverSeq:     1,
	}
	err = ls.PutLadleData(ctx, ldatanew)
	if err != nil {
		return fmt.Errorf("failed to save ladle data in store: %v", err)
	}

	return nil
}

// next keeper's free port
func issueKeeperPort(nl *NodeLayout, ld *LadleData) int {
	max := 0
	for _, k := range nl.Keepers {
		if max == 0 || k.PgPort > max {
			max = k.PgPort
		}
	}
	if max == 0 {
		return ld.Spec.KeepersInitialPort
	} else {
		return max + 1
	}
}

// next value of Clover sequence
func issueCloverSeq(ldata *LadleData) int {
	res := ldata.CloverSeq
	ldata.CloverSeq++
	return res
}

func (ls *LadleStore) AddNodes(ctx context.Context, hl *shmnlog.Logger, nodes []string) error {
	ldata, _, cldata, _, err := ls.GetLadleAndClusterData(ctx)
	if err != nil {
		return err
	}

	// add nodes to map, checking for duplicates
	for _, node := range nodes {
		if _, ok := ldata.Layout[node]; ok {
			return fmt.Errorf("node %v is already in the cluster", node)
		}
		ldata.Layout[node] = &NodeLayout{
			Keepers: make([]Keeper, 0),
			Monitor: false,
			Proxy:   nil,
		}
	}

	// now the clover logic
	nCopies := *ldata.Spec.Repfactor + 1
	if len(nodes)%nCopies != 0 {
		return fmt.Errorf("with clover policy, num of added nodes must be multiple of num of copies stored (%d)", nCopies)
	}

	hl.Infof("Initting Stolon instances...")
	newCloverIds := make([]int, 0)
	nNewClovers := len(nodes) / nCopies
	// configure each new clover
	for i := 0; i < nNewClovers; i++ {
		newCloverSlice := nodes[i*nCopies : (i+1)*nCopies]
		newClover := make([]string, nCopies)
		copy(newClover, newCloverSlice)
		newCloverId := issueCloverSeq(ldata)
		ldata.Clovers[newCloverId] = newClover
		newCloverIds = append(newCloverIds, newCloverId)

		// configure nCopies repgroups; each clover member must be master of some repgroup
		for j := 0; j < nCopies; j++ {
			master := newClover[j]
			rgName := fmt.Sprintf("clover-%d-%s", newCloverId, master)
			rg := cluster.RepGroup{
				StolonName: rgName,
				// always use single (shardman) store
				StoreConnInfo: cluster.StoreConnInfo{Endpoints: ""},
				StorePrefix:   "stolon/cluster",
			}
			// add keepers, sentinels and proxies to each node of the clover
			for keeperUid := 0; keeperUid < nCopies; keeperUid++ {
				node := newClover[keeperUid]
				keeper := Keeper{
					Id:        KeeperId{RepGroup: rgName, Uid: keeperUid},
					Preferred: false,
					PgPort:    issueKeeperPort(ldata.Layout[node], ldata),
				}
				// make master actually master and put proxy on it
				if node == master {
					keeper.Preferred = true
					if cldata.Spec.UseProxy {
						ldata.Layout[node].Proxy = &Proxy{
							RepGroup: rgName,
							Port:     ldata.Spec.ProxyPort,
						}
					}
				}
				// push the keeper
				ldata.Layout[node].Keepers = append(ldata.Layout[node].Keepers, keeper)
			}

			// actually create stolon instance
			err = cluster.StolonInit(ldata.Spec.StoreConnInfo, &rg, &cldata.Spec.StolonSpec, ldata.Spec.StolonBinPath)
			if err != nil {
				return err
			}
		}
	}

	// add monitors, if needed
	for _, node := range nodes {
		if ldata.MonitorsNum >= *ldata.Spec.MonitorsNum {
			break
		}
		ldata.Layout[node].Monitor = true
	}

	// push ldata to the store, triggering bowls to spin up daemons
	err = ls.PutLadleData(ctx, ldata)
	if err != nil {
		return fmt.Errorf("failed to save ladle data in store: %v", err)
	}

	// now, before actually adding repgroups, we must wait until keepers get
	// up and running, which takes quite a bit of time
	hl.Infof("Waiting for keepers/proxies to start... make sure bowl daemons are running on the nodes")
	for _, cloverId := range newCloverIds {
		clover := ldata.Clovers[cloverId]
		for _, master := range clover {
			rgName := fmt.Sprintf("clover-%d-%s", cloverId, master)
			rg := cluster.RepGroup{
				StolonName: rgName,
				// always use single (shardman) store
				StoreConnInfo: cluster.StoreConnInfo{Endpoints: ""},
				StorePrefix:   "stolon/cluster",
			}

			var conn *pgx.Conn = nil
			for {
				var err error
				var connstr string
				var connconfig pgx.ConnConfig
				var inRecovery bool

				connstr, err = pg.GetSuConnstr(ctx, ls.ClusterStore, &rg, cldata)
				if err != nil {
					if _, ok := err.(cluster.MasterUnavailableError); ok {
						goto notAvailableYet
					}
					return fmt.Errorf("failed to check connstring of new rg: %v", err)
				}

				connconfig, err = pgx.ParseConnectionString(connstr)
				if err != nil {
					return fmt.Errorf("connstring parsing \"%s\" failed: %v", connstr, err) // should not happen
				}
				conn, err = pgx.Connect(connconfig)
				if err != nil {
					goto notAvailableYet
				}

				// run something, just in case...
				err = conn.QueryRow("select pg_is_in_recovery()").Scan(&inRecovery)
				if err != nil || inRecovery {
					// actually, should never be in recovery here
					goto notAvailableYet
				}

				// done
				conn.Close()
				break
			notAvailableYet:
				if conn != nil {
					conn.Close()
					conn = nil
				}
				hl.Infof("Waiting for keepers/proxies of rg %s to start... err: %v", rgName, err)
				time.Sleep(2 * time.Second)
			}
		}
	}

	hl.Infof("Adding repgroups...")
	for _, cloverId := range newCloverIds {
		clover := ldata.Clovers[cloverId]
		for _, master := range clover {
			rgName := fmt.Sprintf("clover-%d-%s", cloverId, master)
			rg := cluster.RepGroup{
				StolonName: rgName,
				// always use single (shardman) store
				StoreConnInfo: cluster.StoreConnInfo{Endpoints: ""},
				StorePrefix:   "stolon/cluster",
			}
			err = commands.AddRepGroup(ctx, hl, ls.ClusterStore, ldata.Spec.StoreConnInfo, &rg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// make sure that stolon instances are created for all replication groups
func (ls *LadleStore) FixRepGroups(ctx context.Context, hl *shmnlog.Logger) error {
	ldata, _, cldata, _, err := ls.GetLadleAndClusterData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get ladle/cluster data: %v", err)
	}
	if ldata == nil {
		return fmt.Errorf("no data for cluster %v", ls.ClusterName)
	}

	rgs, _, err := ls.GetRepGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to get rgs: %v", err)
	}

	// we will check each rg several times, but that's not expensive
	for _, layout := range ldata.Layout {
		for _, k := range layout.Keepers {
			rg := cluster.RepGroup{
				StolonName: k.Id.RepGroup,
				// always use single (shardman) store
				StoreConnInfo: cluster.StoreConnInfo{Endpoints: ""},
				StorePrefix:   "stolon/cluster",
			}

			// Create stolon instance, if not yet. Actually,
			// currently they always exist...
			ss := cluster.NewStolonStoreFromExisting(&rg, ls.Store)
			scldata, err := ss.GetClusterData(ctx)
			if err != nil {
				return fmt.Errorf("error getting stolon cluster data: %v", err)
			}
			if scldata == nil {
				hl.Infof("Creating Stolon instance %v", rg.StolonName)
				err = cluster.StolonInit(ldata.Spec.StoreConnInfo, &rg, &cldata.Spec.StolonSpec, ldata.Spec.StolonBinPath)
				if err != nil {
					return err
				}
			}

			// and register it, if not yet
			found := false
			for _, existingRg := range rgs {
				if existingRg.StolonName == rg.StolonName {
					found = true
					break
				}
			}
			if !found {
				hl.Infof("Adding repgroup %v", rg.StolonName)
				err = commands.AddRepGroup(ctx, hl, ls.ClusterStore, ldata.Spec.StoreConnInfo, &rg)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (ls *LadleStore) RmNodes(ctx context.Context, hl *shmnlog.Logger, nodes []string) error {
	ldata, _, err := ls.GetLadleData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get ladle/cluster data: %v", err)
	}
	if ldata == nil {
		return fmt.Errorf("no data for cluster %v", ls.ClusterName)
	}

	// First, calculate which clovers we are going to remove
	rmClovers := map[int][]string{}
	for cloverId, clover := range ldata.Clovers {
		// remove this clover?
		remove := false
	CloverMembersLoop:
		for _, node := range clover {
			for _, n := range nodes {
				if n == node {
					remove = true
					break CloverMembersLoop
				}
			}
		}
		if remove {
			hl.Infof("Removing clover %v with members %v", cloverId, strings.Join(clover, ", "))
			rmClovers[cloverId] = clover
		}
	}

	rgs, _, err := ls.GetRepGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to get rgs: %v", err)
	}

	// And actually remove
	for cloverId, clover := range rmClovers {
		delete(ldata.Clovers, cloverId)
		// remove repgroup for which this member is master, if it exists
		for _, master := range clover {
			rgName := fmt.Sprintf("clover-%d-%s", cloverId, master)

			repGroupExists := false
			for _, existingRg := range rgs {
				if existingRg.StolonName == rgName {
					repGroupExists = true
					break
				}
			}

			if repGroupExists {
				hl.Infof("Removing repgroup %v", rgName)
				err = commands.RmRepGroup(ctx, hl, ls.ClusterStore, rgName)
				if err != nil {
					return err
				}
			}

			// and remove member from metadata
			delete(ldata.Layout, master)
		}
	}

	// push updated ldata to the store
	err = ls.PutLadleData(ctx, ldata)
	if err != nil {
		return fmt.Errorf("failed to save ladle data in store: %v", err)
	}

	return nil
}
