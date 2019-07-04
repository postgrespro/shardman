// Copyright (c) 2018, Postgres Professional

package cluster

const (
	CurrentFormatVersion = 1
)

// Global cluster data
type ClusterData struct {
	FormatVersion uint64
	Spec          ClusterSpec
}

type ClusterSpec struct {
	// Same su user auth info is assumed in all repgroups
	PgSuAuthMethod   string
	PgSuPassword     string
	PgSuUsername     string
	PgReplAuthMethod string
	PgReplPassword   string
	PgReplUsername   string
	// It is here not in ladle because things not knowing about ladle at all
	// (e.g. monitor, addrepgroup) need to get access to current master
	// connstr, and this defines whether we use proxy or not. Thus, you can
	// interpret it as 'whether added rgs are expected to have proxy which
	// we will use'.
	// Accordingly, ladle checks this and configures proxies
	// if needed.
	UseProxy   bool
	StolonSpec StolonSpec
}

// Replication group ~ Stolon instance.
type RepGroup struct {
	StolonName string
	// if StoreEndpoints is "", shardman store is assumed
	StoreConnInfo StoreConnInfo
	StorePrefix   string
	SysId         int64
}

// Sharded tables
type Table struct {
	Schema              string
	Relname             string // unquoted
	Nparts              int
	Partmap             []int // part num -> repgroup id mapping
	ColocateWithSchema  string
	ColocateWithRelname string // bind this table parts distribution to some other table
}
