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
	GetRepGroups(ctx context.Context) (map[int]*cluster.RepGroupData, *KVPair, error)
}

type ClusterStoreImpl struct {
	storePath string
	store     etcdV3Store
}

func NewClusterStore(cfg *cmdcommon.CommonConfig) (*ClusterStoreImpl, error) {
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
	return &ClusterStoreImpl{storePath: storePath, store: etcdstore}, nil
}

func (cs *ClusterStoreImpl) GetRepGroups(ctx context.Context) (map[int]*cluster.RepGroupData, *KVPair, error) {
	var rgdata map[int]*cluster.RepGroupData
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

func (cs *ClusterStoreImpl) Close() error {
	return cs.store.Close()
}
