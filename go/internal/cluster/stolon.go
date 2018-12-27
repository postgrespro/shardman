// Copyright (c) 2018, Postgres Professional

// Some functions to work with Stolon data. Yeah, peeking private data is not
// nice, but Stolon doesn't expose any API (no way to include internal packages)
// and exec'ing stolonctl is not nice either
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"crypto/tls"

	etcdclientv3 "go.etcd.io/etcd/clientv3"
	"postgrespro.ru/hodgepodge/internal/store"
	tlswrap "postgrespro.ru/hodgepodge/internal/tls"
)

type StolonStore struct {
	storePath string
	store     store.EtcdV3Store
}

func NewStolonStore(rg *RepGroup) (*StolonStore, error) {
	var tlsConfig *tls.Config = nil
	var err error
	endpoints := strings.Split(rg.StoreConnInfo.Endpoints, ",")

	for _, endp := range endpoints {
		if strings.HasPrefix(endp, "https") {
			tlsConfig, err = tlswrap.NewTLSConfig(rg.StoreConnInfo.CertFile, rg.StoreConnInfo.Key,
				rg.StoreConnInfo.CAFile, false)
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
	etcdstore := store.NewEtcdV3Store(cli)
	storePath := filepath.Join(rg.StorePrefix, rg.StolonName)
	return &StolonStore{storePath: storePath, store: etcdstore}, nil
}

// use given store
func NewStolonStoreFromExisting(rg *RepGroup, store store.EtcdV3Store) *StolonStore {
	storePath := filepath.Join(rg.StorePrefix, rg.StolonName)
	return &StolonStore{storePath: storePath, store: store}
}

// Copy needed fragments from Stolon...
// Copyright 2015 Sorint.lab under Apache License, Version 2.0 (in licenses/apache-2.0)
type StolonClusterData struct {
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

// Master connection info
type Endpoint struct {
	Address string
	Port    string
}

func (ss *StolonStore) GetClusterData(ctx context.Context) (*StolonClusterData, error) {
	var clusterData StolonClusterData

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
	return &clusterData, nil
}

// if no master available or there is no cluster (but store is ok), returns nil, nil
func (ss *StolonStore) GetMaster(ctx context.Context) (*Endpoint, error) {
	clusterData, err := ss.GetClusterData(ctx)
	if err != nil {
		return nil, err
	}
	if clusterData == nil {
		return nil, nil
	}

	var master = &Endpoint{}
	if db, ok := clusterData.DBs[clusterData.Proxy.Spec.MasterDBUID]; ok {
		master.Address = db.Status.ListenAddress
		master.Port = db.Status.Port
		return master, nil
	} else {
		return nil, nil
	}
}

// if no proxy available (but store is ok) returns nil, nil
func (ss *StolonStore) GetProxy(ctx context.Context) (*Endpoint, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ss *StolonStore) Close() error {
	return ss.store.Close()
}

// type PgClusterSpec struct {
// PGParameters map[string]string `json:"pgParameters,omitempty"`
// PGHBA        []string          `json:"pgHBA,omitempty"`
// }

// get store conn info for given repgroup: it is either hodgepodge's store conn
// info or separately configured info. We need all this ugly stuff only because
// we run e.g. stolonctl update directly instead of mocking with its private
// data
func getStolonStoreConnInfo(hpc *StoreConnInfo, rg *RepGroup) StoreConnInfo {
	if rg.StoreConnInfo.Endpoints == "" {
		return *hpc
	} else {
		return rg.StoreConnInfo
	}
}

func getConnArgs(hpc *StoreConnInfo, rg *RepGroup) []string {
	args := []string{"--store-backend", "etcdv3"}
	ci := getStolonStoreConnInfo(hpc, rg)

	if ci.Endpoints != "" {
		args = append(args, "--store-endpoints", ci.Endpoints)
	}
	if ci.CAFile != "" {
		args = append(args, "--store-ca-file", ci.CAFile)
	}
	if ci.CertFile != "" {
		args = append(args, "--store-cert-file", ci.CertFile)
	}
	if ci.Key != "" {
		args = append(args, "--store-key", ci.Key)
	}
	return args
}

// Okay, let's try to avoid touching internals this time
func StolonUpdate(hpc *StoreConnInfo, rg *RepGroup, rgid int, patch bool, spec *StolonSpec) error {
	// Add repgroup-specific stuff
	spec.PGParameters["hodgepodge.rgid"] = strconv.Itoa(rgid)

	specj, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	cmdargs := getConnArgs(hpc, rg)
	cmdargs = append(cmdargs, "--cluster-name", rg.StolonName, "--store-prefix", rg.StorePrefix)

	cmdargs = append(cmdargs, "update")
	if patch {
		cmdargs = append(cmdargs, "--patch")
	}
	cmdargs = append(cmdargs, string(specj))
	// fmt.Printf("cmd is %v\n", cmdargs)
	// TODO: allow to configure path to stolon binary
	cmd := exec.Command("stolonctl", cmdargs...)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("stolonctl update failed, stdout/err: %s, err: %s",
			string(stdoutStderr), err.Error())
	}
	return nil
}

// And this time too
func StolonInit(hpc *StoreConnInfo, rg *RepGroup, spec *StolonSpec, stolonBinPath string) error {
	specj, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	cmdargs := getConnArgs(hpc, rg)
	cmdargs = append(cmdargs, "--cluster-name", rg.StolonName, "--store-prefix", rg.StorePrefix)
	cmdargs = append(cmdargs, "init")
	cmdargs = append(cmdargs, "--yes")
	cmdargs = append(cmdargs, string(specj))

	cmd := exec.Command(filepath.Join(stolonBinPath, "stolonctl"), cmdargs...)
	// log.Printf("DEBUG: running %s", strings.Join(cmdargs, " "))
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("stolonctl init failed, cmd: %s, stdout/err: %s, err: %s",
			strings.Join(cmdargs, " "), string(stdoutStderr), err.Error())
	}
	return nil

}

// From Stolon
// Copyright 2015 Sorint.lab under Apache License, Version 2.0 (in licenses/apache-2.0)
// Duration is needed to be able to marshal/unmarshal json strings with time
// unit (eg. 3s, 100ms) instead of ugly times in nanoseconds.
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	du, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = du
	return nil
}

type PGParameters map[string]string

type ClusterInitMode string

type ClusterRole string

type NewConfig struct {
	Locale        string `json:"locale,omitempty"`
	Encoding      string `json:"encoding,omitempty"`
	DataChecksums bool   `json:"dataChecksums,omitempty"`
}

type PITRConfig struct {
	// DataRestoreCommand defines the command to execute for restoring the db
	// cluster data). %d is replaced with the full path to the db cluster
	// datadir. Use %% to embed an actual % character.
	DataRestoreCommand      string                   `json:"dataRestoreCommand,omitempty"`
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
	RecoveryTargetSettings  *RecoveryTargetSettings  `json:"recoveryTargetSettings,omitempty"`
}

type ExistingConfig struct {
	KeeperUID string `json:"keeperUID,omitempty"`
}

// Standby config when role is standby
type StandbyConfig struct {
	StandbySettings         *StandbySettings         `json:"standbySettings,omitempty"`
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
}

// ArchiveRecoverySettings defines the archive recovery settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/archive-recovery-settings.html )
type ArchiveRecoverySettings struct {
	// value for restore_command
	RestoreCommand string `json:"restoreCommand,omitempty"`
}

// RecoveryTargetSettings defines the recovery target settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/recovery-target-settings.html )
type RecoveryTargetSettings struct {
	RecoveryTarget         string `json:"recoveryTarget,omitempty"`
	RecoveryTargetLsn      string `json:"recoveryTargetLsn,omitempty"`
	RecoveryTargetName     string `json:"recoveryTargetName,omitempty"`
	RecoveryTargetTime     string `json:"recoveryTargetTime,omitempty"`
	RecoveryTargetXid      string `json:"recoveryTargetXid,omitempty"`
	RecoveryTargetTimeline string `json:"recoveryTargetTimeline,omitempty"`
}

// StandbySettings defines the standby settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/standby-settings.html )
type StandbySettings struct {
	PrimaryConninfo       string `json:"primaryConninfo,omitempty"`
	PrimarySlotName       string `json:"primarySlotName,omitempty"`
	RecoveryMinApplyDelay string `json:"recoveryMinApplyDelay,omitempty"`
}

type SUReplAccessMode string

type StolonSpec struct {
	// Interval to wait before next check
	SleepInterval *Duration `json:"sleepInterval,omitempty"`
	// Time after which any request (keepers checks from sentinel etc...) will fail.
	RequestTimeout *Duration `json:"requestTimeout,omitempty"`
	// Interval to wait for a db to be converged to the required state when
	// no long operation are expected.
	ConvergenceTimeout *Duration `json:"convergenceTimeout,omitempty"`
	// Interval to wait for a db to be initialized (doing a initdb)
	InitTimeout *Duration `json:"initTimeout,omitempty"`
	// Interval to wait for a db to be synced with a master
	SyncTimeout *Duration `json:"syncTimeout,omitempty"`
	// Interval after the first fail to declare a keeper or a db as not healthy.
	FailInterval *Duration `json:"failInterval,omitempty"`
	// Interval after which a dead keeper will be removed from the cluster data
	DeadKeeperRemovalInterval *Duration `json:"deadKeeperRemovalInterval,omitempty"`
	// Max number of standbys. This needs to be greater enough to cover both
	// standby managed by stolon and additional standbys configured by the
	// user. Its value affect different postgres parameters like
	// max_replication_slots and max_wal_senders. Setting this to a number
	// lower than the sum of stolon managed standbys and user managed
	// standbys will have unpredicatable effects due to problems creating
	// replication slots or replication problems due to exhausted wal
	// senders.
	MaxStandbys *uint16 `json:"maxStandbys,omitempty"`
	// Max number of standbys for every sender. A sender can be a master or
	// another standby (if/when implementing cascading replication).
	MaxStandbysPerSender *uint16 `json:"maxStandbysPerSender,omitempty"`
	// Max lag in bytes that an asynchronous standy can have to be elected in
	// place of a failed master
	MaxStandbyLag *uint32 `json:"maxStandbyLag,omitempty"`
	// Use Synchronous replication between master and its standbys
	SynchronousReplication *bool `json:"synchronousReplication,omitempty"`
	// MinSynchronousStandbys is the mininum number if synchronous standbys
	// to be configured when SynchronousReplication is true
	MinSynchronousStandbys *uint16 `json:"minSynchronousStandbys,omitempty"`
	// MaxSynchronousStandbys is the maximum number if synchronous standbys
	// to be configured when SynchronousReplication is true
	MaxSynchronousStandbys *uint16 `json:"maxSynchronousStandbys,omitempty"`
	// AdditionalWalSenders defines the number of additional wal_senders in
	// addition to the ones internally defined by stolon
	AdditionalWalSenders *uint16 `json:"additionalWalSenders,omitempty"`
	// AdditionalMasterReplicationSlots defines additional replication slots to
	// be created on the master postgres instance. Replication slots not defined
	// here will be dropped from the master instance (i.e. manually created
	// replication slots will be removed).
	AdditionalMasterReplicationSlots []string `json:"additionalMasterReplicationSlots,omitempty"`
	// Whether to use pg_rewind
	UsePgrewind *bool `json:"usePgrewind,omitempty"`
	// InitMode defines the cluster initialization mode. Current modes are: new, existing, pitr
	InitMode *ClusterInitMode `json:"initMode,omitempty"`
	// Whether to merge pgParameters of the initialized db cluster, useful
	// the retain initdb generated parameters when InitMode is new, retain
	// current parameters when initMode is existing or pitr.
	MergePgParameters *bool `json:"mergePgParameters,omitempty"`
	// Role defines the cluster operating role (master or standby of an external database)
	Role *ClusterRole `json:"role,omitempty"`
	// Init configuration used when InitMode is "new"
	NewConfig *NewConfig `json:"newConfig,omitempty"`
	// Point in time recovery init configuration used when InitMode is "pitr"
	PITRConfig *PITRConfig `json:"pitrConfig,omitempty"`
	// Existing init configuration used when InitMode is "existing"
	ExistingConfig *ExistingConfig `json:"existingConfig,omitempty"`
	// Standby config when role is standby
	StandbyConfig *StandbyConfig `json:"standbyConfig,omitempty"`
	// Define the mode of the default hba rules needed for replication by standby keepers (the su and repl auth methods will be the one provided in the keeper command line options)
	// Values can be "all" or "strict", "all" allow access from all ips, "strict" restrict master access to standby servers ips.
	// Default is "all"
	DefaultSUReplAccessMode *SUReplAccessMode `json:"defaultSUReplAccessMode,omitempty"`
	// Map of postgres parameters
	PGParameters PGParameters `json:"pgParameters,omitempty"`
	// Additional pg_hba.conf entries
	// we don't set omitempty since we want to distinguish between null or empty slice
	PGHBA []string `json:"pgHBA"`
	// Enable automatic pg restart when pg parameters that requires restart changes
	AutomaticPgRestart *bool `json:"automaticPgRestart,omitempty"`
}
