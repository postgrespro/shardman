package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/store"
)

// common args
type CommonConfig struct {
	StoreEndpoints string
	ClusterName    string
	// TODO: tls
}

func AddCommonFlags(cmd *cobra.Command, cfg *CommonConfig) {
	cmd.PersistentFlags().StringVar(&cfg.StoreEndpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0],
		"a comma-delimited list of store endpoints (use https scheme for tls communication)")
	cmd.PersistentFlags().StringVar(&cfg.ClusterName, "cluster-name", "", "cluster name")
}

// check options
func CheckConfig(cfg *CommonConfig) error {
	if cfg.ClusterName == "" {
		return fmt.Errorf("cluster name required")
	}

	return nil
}

func NewClusterStore(cfg *CommonConfig) (store.ClusterStore, error) {
	return store.NewClusterStore(cfg.StoreEndpoints, cfg.ClusterName)
}
