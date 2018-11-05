// Some functions to work with Stolon data. Yeah, peeking private data is not
// nice, but Stolon doesn't expose any API (no way to include internal packages)
// and exec'ing stolonctl is not nice either
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"crypto/tls"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
	"postgrespro.ru/hodgepodge/internal/cluster"
	tlswrap "postgrespro.ru/hodgepodge/internal/tls"
)

type StolonStore struct {
	storePath string
	store     EtcdV3Store
}

func NewStolonStore(rg *cluster.RepGroup) (*StolonStore, error) {
	var tlsConfig *tls.Config = nil
	var err error
	endpoints := strings.Split(rg.StoreEndpoints, ",")

	for _, endp := range endpoints {
		if strings.HasPrefix(endp, "https") {
			tlsConfig, err = tlswrap.NewTLSConfig(rg.StoreCertFile, rg.StoreKey,
				rg.StoreCAFile, rg.StoreSkipTLSVerify)
			if err != nil {
				return nil, fmt.Errorf("cannot create Stolon store tls config: %v", err)

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
	etcdstore := NewEtcdV3Store(cli)
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

type ClusterSpec struct {
	PGParameters map[string]string `json:"pgParameters,omitempty"`
	PGHBA        []string          `json:"pgHBA,omitempty"`
}

// Okay, let's try to avoid touching internals this time
func StolonUpdate(rg *cluster.RepGroup, patch bool, spec *cluster.StolonSpec) error {
	specj, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	cmdargs := []string{"--cluster-name", rg.StolonName,
		"--store-backend", "etcdv3", "--store-prefix", rg.StorePrefix}
	if rg.StoreEndpoints != "" {
		cmdargs = append(cmdargs, "--store-endpoints", rg.StoreEndpoints)
	}
	if rg.StoreCAFile != "" {
		cmdargs = append(cmdargs, "--store-ca-file", rg.StoreCAFile)
	}
	if rg.StoreCertFile != "" {
		cmdargs = append(cmdargs, "--store-cert-file", rg.StoreCertFile)
	}
	if rg.StoreKey != "" {
		cmdargs = append(cmdargs, "--store-key", rg.StoreKey)
	}
	if rg.StoreSkipTLSVerify {
		cmdargs = append(cmdargs, "--store-skip-tls-verify", rg.StoreKey)

	}
	cmdargs = append(cmdargs, "update")
	if patch {
		cmdargs = append(cmdargs, "--patch")
	}
	cmdargs = append(cmdargs, string(specj))
	// fmt.Printf("cmd is %v\n", cmdargs)
	cmd := exec.Command("stolonctl", cmdargs...)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("stolonctl update failed, stdout/err: %s, err: %s",
			string(stdoutStderr), err.Error())
	}
	return nil
}
