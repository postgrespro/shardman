// Some functions to retrieve Stolon data. Yeah, peeking private data is not
// nice, but Stolon doesn't expose any API (no way to include internal packages)
// and exec'ing stolonctl is worse
package stolonstore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/store"
)

type StolonStore struct {
	storePath string
	store     store.EtcdV3Store
}

func NewStolonStore(rg *cluster.RepGroup) (*StolonStore, error) {
	endpoints := strings.Split(rg.StoreEndpoints, ",")

	cli, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: endpoints,
		TLS:       nil,
	})
	if err != nil {
		return nil, err
	}
	etcdstore := store.NewEtcdV3Store(cli)
	storePath := filepath.Join(rg.StorePrefix, rg.StolonName)
	return &StolonStore{storePath: storePath, store: etcdstore}, nil
}

// Copy needed fragments from Stolon...
type ClusterData struct {
	DBs   map[string]*DB `json:"dbs"`
	Proxy *Proxy         `json:"proxy"`
}
type DB struct {
	Status DBStatus `json:"status,omitempty"`
}
type DBStatus struct {
	ListenAddress string `json:"listenAddress,omitempty"`
	Port          string `json:"port,omitempty"`
}
type Proxy struct {
	Spec ProxySpec `json:"spec,omitempty"`
}
type ProxySpec struct {
	MasterDBUID string `json:"masterDbUid,omitempty"`
}

func (ss *StolonStore) GetMaster(ctx context.Context) (*cluster.Master, error) {
	var master = &cluster.Master{}
	var clusterData ClusterData

	path := filepath.Join(ss.storePath, "clusterdata")
	pair, err := ss.store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, nil
	}
	if err := json.Unmarshal(pair.Value, &clusterData); err != nil {
		return nil, err
	}
	if db, ok := clusterData.DBs[clusterData.Proxy.Spec.MasterDBUID]; ok {
		master.ListenAddress = db.Status.ListenAddress
		master.Port = db.Status.Port
	}
	return master, nil
}

func (ss *StolonStore) Close() error {
	return ss.store.Close()
}
