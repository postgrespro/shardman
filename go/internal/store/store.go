// Stuff for retrieving metadata
package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
)

// KVPair represents {Key, Value, Lastindex} tuple
type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}

type ClusterStore interface {
	GetRepGroups(ctx context.Context) (map[int]*cluster.RepGroup, *KVPair, error)
}

type clusterStoreImpl struct {
	storePath string
	store     etcdV3Store
}

func NewClusterStore(cfg *cmdcommon.CommonConfig) (*clusterStoreImpl, error) {
	var endpoints []string

	if cfg.StoreEndpoints == "" {
		endpoints = DefaultEtcdEndpoints[:]
	} else {
		endpoints = strings.Split(cfg.StoreEndpoints, ",")
	}

	cli, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: endpoints,
		TLS:       nil,
	})
	if err != nil {
		return nil, err
	}
	etcdstore := etcdV3Store{c: cli}
	storePath := filepath.Join("hodgepodge", cfg.ClusterName)
	return &clusterStoreImpl{storePath: storePath, store: etcdstore}, nil
}

// Get global cluster data
func (cs *clusterStoreImpl) GetClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	var cldata = &cluster.ClusterData{}
	path := filepath.Join(cs.storePath, "clusterdata")
	pair, err := cs.store.Get(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	if pair == nil {
		return nil, nil, nil
	}
	if err := json.Unmarshal(pair.Value, cldata); err != nil {
		return nil, nil, err
	}
	return cldata, pair, nil
}

// Put global cluster data
func (cs *clusterStoreImpl) PutClusterData(ctx context.Context, cldata cluster.ClusterData) error {
	cldataj, err := json.Marshal(cldata)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.storePath, "clusterdata")
	return cs.store.Put(ctx, path, cldataj)
}

// Get all Stolons connection info
func (cs *clusterStoreImpl) GetRepGroups(ctx context.Context) (map[int]*cluster.RepGroup, *KVPair, error) {
	var rgdata map[int]*cluster.RepGroup
	path := filepath.Join(cs.storePath, "repgroups")
	pair, err := cs.store.Get(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	if pair == nil {
		return nil, nil, nil
	}
	if err := json.Unmarshal(pair.Value, &rgdata); err != nil {
		return nil, nil, err
	}
	return rgdata, pair, nil
}

// Put replication groups info
func (cs *clusterStoreImpl) PutRepGroups(ctx context.Context, rgs map[int]*cluster.RepGroup) error {
	rgsj, err := json.Marshal(rgs)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.storePath, "repgroups")
	return cs.store.Put(ctx, path, rgsj)
}

// Save info about sharded tables
func (cs *clusterStoreImpl) PutTables(ctx context.Context, tables []cluster.Table) error {
	tablesj, err := json.Marshal(tables)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.storePath, "tables")
	return cs.store.Put(ctx, path, tablesj)
}

// Save current masters for each repgroup
func (cs *clusterStoreImpl) PutMasters(ctx context.Context, masters map[int]*cluster.Master) error {
	mastersj, err := json.Marshal(masters)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.storePath, "masters")
	return cs.store.Put(ctx, path, mastersj)
}

func (cs *clusterStoreImpl) Close() error {
	return cs.store.Close()
}
