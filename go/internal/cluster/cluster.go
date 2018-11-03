package cluster

// type ClusterData struct {
// FormatVersion uint64    `json:"formatVersion"`
// ChangeTime    time.Time `json:"changeTime"`
// }

// Replication group ~ Stolon instance.
type RepGroupData struct {
	StoreEndpoints string
	StolonName     string
}
