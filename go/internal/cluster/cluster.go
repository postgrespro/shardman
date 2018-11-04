package cluster

const (
	CurrentFormatVersion = 1
)

// Global cluster data
type ClusterData struct {
	FormatVersion uint64 `json:"formatVersion"`
	// Same su user auth info is assumed in all repgroups
	PgSuAuthMethod string
	PgSuPassword   string
	PgSuUsername   string
}

// Replication group ~ Stolon instance.
type RepGroup struct {
	StolonName     string
	StoreEndpoints string
	StoreCAFile    string
	StoreCertFile  string
	StoreKey       string
	StorePrefix    string
}

// Sharded tables
type Table struct {
	Relname string
	Sql     string // CREATE TABLE statement
	Nparts  int
	Partmap []int // part num -> repgroup id mapping
}

// Master connection info
type Master struct {
	ListenAddress string
	Port          string
}
