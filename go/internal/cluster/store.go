// Copyright (c) 2018, Postgres Professional

// retrieving cluster data from the store
package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"postgrespro.ru/shardman/internal/store"
	tlswrap "postgrespro.ru/shardman/internal/tls"
)

type ClusterStore struct {
	// these are exported to use in ladle
	StorePath   string
	Store       store.EtcdV3Store
	ClusterName string // mainly for logging
}

type ClusterStoreConnInfo struct {
	ClusterName   string
	StoreConnInfo StoreConnInfo
}

type StoreConnInfo struct {
	Endpoints string
	CAFile    string
	// client auth
	CertFile string // client's cert
	Key      string // client's private key
}

func NewClusterStore(cfg *ClusterStoreConnInfo) (*ClusterStore, error) {
	endpoints := strings.Split(cfg.StoreConnInfo.Endpoints, ",")

	var tlsConfig *tls.Config = nil
	var err error
	for _, endp := range endpoints {
		if strings.HasPrefix(endp, "https") {
			tlsConfig, err = tlswrap.NewTLSConfig(cfg.StoreConnInfo.CertFile, cfg.StoreConnInfo.Key,
				cfg.StoreConnInfo.CAFile, false)
			if err != nil {
				return nil, fmt.Errorf("cannot create store tls config: %v", err)

			}
			break
		}
	}

	cli, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: endpoints,
		TLS:       tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	etcdstore := store.NewEtcdV3Store(cli)
	storePath := filepath.Join("shardman", cfg.ClusterName)
	return &ClusterStore{StorePath: storePath, Store: etcdstore, ClusterName: cfg.ClusterName}, nil
}

// Get global cluster data
func (cs *ClusterStore) GetClusterData(ctx context.Context) (*ClusterData, *store.KVPair, error) {
	var cldata = &ClusterData{}
	path := filepath.Join(cs.StorePath, "clusterdata")
	pair, err := cs.Store.Get(ctx, path)
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
func (cs *ClusterStore) PutClusterData(ctx context.Context, cldata *ClusterData) error {
	cldataj, err := json.Marshal(cldata)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.StorePath, "clusterdata")
	return cs.Store.Put(ctx, path, cldataj)
}

// Get all Stolons connection info
func (cs *ClusterStore) GetRepGroups(ctx context.Context) (map[int]*RepGroup, *store.KVPair, error) {
	var rgdata map[int]*RepGroup
	path := filepath.Join(cs.StorePath, "repgroups")
	pair, err := cs.Store.Get(ctx, path)
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
func (cs *ClusterStore) PutRepGroups(ctx context.Context, rgs map[int]*RepGroup) error {
	rgsj, err := json.Marshal(rgs)
	if err != nil {
		return err
	}
	path := filepath.Join(cs.StorePath, "repgroups")
	return cs.Store.Put(ctx, path, rgsj)
}

// Save current masters for each repgroup
// func (cs *ClusterStore) PutMasters(ctx context.Context, masters map[int]*Master) error {
// mastersj, err := json.Marshal(masters)
// if err != nil {
// return err
// }
// path := filepath.Join(cs.StorePath, "masters")
// return cs.Store.Put(ctx, path, mastersj)
// }

func (cs *ClusterStore) Close() error {
	return cs.Store.Close()
}

func patchStolonSpec(spec *StolonSpec, patch []byte) (*StolonSpec, error) {
	specj, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster spec: %v", err)
	}

	newspecj, err := strategicpatch.StrategicMergePatch(specj, patch, &StolonSpec{})
	if err != nil {
		return nil, fmt.Errorf("failed to merge patch cluster spec: %v", err)
	}
	var newspec *StolonSpec
	if err := json.Unmarshal(newspecj, &newspec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal patched cluster spec: %v", err)
	}
	return newspec, nil
}

// Broadcast new stolon spec to all stolons and update it in store
func (cs *ClusterStore) UpdateStolonSpec(ctx context.Context, hpc *StoreConnInfo, specdata []byte, patch bool) error {
	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return err
	}

	currentspec := &cldata.Spec.StolonSpec
	var newspec *StolonSpec
	if patch {
		newspec, err = patchStolonSpec(currentspec, specdata)
		if err != nil {
			return err
		}
	} else {
		newspec = currentspec
	}

	// sj, _ := json.Marshal(newspec)
	// log.Printf("new spec is \n%v", string(sj))
	rgs, _, err := cs.GetRepGroups(ctx)
	if err != nil {
		return err
	}
	for rgid, rg := range rgs {
		// we already patched if needed, just pass new spec. Defaults
		// for unspecified values are set by Stolon.
		if err = StolonUpdate(hpc, rg, rgid, false, newspec); err != nil {
			return err
		}
	}

	cldata.Spec.StolonSpec = *newspec
	return cs.PutClusterData(ctx, cldata)
}

type MasterUnavailableError struct{}

func (mue MasterUnavailableError) Error() string {
	return "no masters found"
}

// Get current connstr for this rg as map of libpq options
// if no master available, returns MasterUnavailableError
func (cs *ClusterStore) GetSuConnstrMap(ctx context.Context, rg *RepGroup, cldata *ClusterData) (map[string]string, error) {
	// if this rg has separate store, connect to it
	var ss *StolonStore
	if rg.StoreConnInfo.Endpoints != "" {
		var err error
		ss, err = NewStolonStore(rg)
		if err != nil {
			return nil, err
		}
		defer ss.Close()
	} else {
		// otherwise, use our
		ss = NewStolonStoreFromExisting(rg, cs.Store)
	}

	var err error
	var ep *Endpoint
	if cldata.Spec.UseProxy {
		ep, err = ss.GetProxy(ctx)
	} else {
		ep, err = ss.GetMaster(ctx)
	}
	if err != nil {
		return nil, err
	}
	if ep == nil {
		return nil, MasterUnavailableError{}
	}

	cp := map[string]string{
		"user":   cldata.Spec.PgSuUsername,
		"dbname": "postgres",
		"host":   ep.Address,
		"port":   ep.Port,
	}
	if cldata.Spec.PgSuAuthMethod != "trust" {
		cp["password"] = cldata.Spec.PgSuPassword
	}
	return cp, nil
}
