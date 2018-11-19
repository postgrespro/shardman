// Copyright (c) 2018, Postgres Professional

package cluster

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	CurrentFormatVersion = 1
)

// Global cluster data
type ClusterData struct {
	FormatVersion uint64
	// Same su user auth info is assumed in all repgroups
	PgSuAuthMethod string
	PgSuPassword   string
	PgSuUsername   string
	StolonSpec     *StolonSpec
}

// Replication group ~ Stolon instance.
type RepGroup struct {
	StolonName         string
	StoreEndpoints     string
	StoreCAFile        string
	StoreCertFile      string
	StoreKey           string
	StoreSkipTLSVerify bool
	StorePrefix        string
	SysId              int64
}

// Sharded tables
type Table struct {
	Relname      string // unquoted
	Sql          string // CREATE TABLE statement
	Nparts       int
	Partmap      []int  // part num -> repgroup id mapping
	ColocateWith string // bind this table parts distribution to some other table
}

// Master connection info
type Master struct {
	ListenAddress string
	Port          string
}

// From Stolon
// Copyright 2015 Sorint.lab under Apache License, Version 2.0
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

// note that we stamp omitempty everywhere, unlike in stolon -- if user hasn't
// specified something in spec patch, it must not be touched
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
	// Stolon doesn't set omitempty since it wants to distinguish between null or empty slice, what?
	PGHBA []string `json:"pgHBA,omitempty"`
	// Enable automatic pg restart when pg parameters that requires restart changes
	AutomaticPgRestart *bool `json:"automaticPgRestart,omitempty"`
}
