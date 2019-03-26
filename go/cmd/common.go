// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/store"
)

// set in Makefile
var ShardmanVersion = "not defined during build"

func AddCommonFlags(cmd *cobra.Command, cfg *cluster.ClusterStoreConnInfo, logLevel *string) {
	cmd.PersistentFlags().StringVar(&cfg.ClusterName, "cluster-name", "", "cluster name")
	cmd.PersistentFlags().StringVar(&cfg.StoreConnInfo.Endpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0],
		"a comma-delimited list of store endpoints (use https scheme for tls communication)")
	cmd.PersistentFlags().StringVar(&cfg.StoreConnInfo.CAFile, "store-ca-file", "",
		"verify certificates of HTTPS-enabled store using this CA bundle")
	cmd.PersistentFlags().StringVar(&cfg.StoreConnInfo.CAFile, "store-cert-file", "",
		"certificate file for client identification to the store")
	cmd.PersistentFlags().StringVar(&cfg.StoreConnInfo.Key, "store-key", "",
		"private key file for client identification to the store")

	cmd.PersistentFlags().StringVar(logLevel, "log-level", "info",
		"error|warn|info|debug")
}

// check options
func CheckConfig(cfg *cluster.ClusterStoreConnInfo) error {
	if cfg.ClusterName == "" {
		return fmt.Errorf("cluster name required")
	}

	return nil
}
