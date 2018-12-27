// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/cluster/commands"
)

// we will store args directly in RepGroup struct
var newrg cluster.RepGroup

var addrgCmd = &cobra.Command{
	Use:   "addrepgroup",
	Run:   addRepGroup,
	Short: "Add replication group to the cluster",
}

func init() {
	rootCmd.AddCommand(addrgCmd)

	addrgCmd.Flags().StringVar(&newrg.StolonName, "stolon-name", "",
		"cluster-name of Stolon instance being added. Must be unique for the whole hodgepodge cluster")
	addrgCmd.Flags().StringVar(&newrg.StoreConnInfo.Endpoints, "store-endpoints",
		"",
		"a comma-delimited list of Stolon store endpoints (use https scheme for tls communication). If empty, assume Stolon data is stored in the same store as hodgepodge, under stolon/cluster prefix; the rest of store-connection options are irrelevant in this case.")
	addrgCmd.Flags().StringVar(&newrg.StoreConnInfo.CAFile, "store-ca-file", "",
		"verify certificates of HTTPS-enabled Stolon store servers using this CA bundle")
	addrgCmd.Flags().StringVar(&newrg.StoreConnInfo.CertFile, "store-cert-file", "",
		"certificate file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&newrg.StoreConnInfo.Key, "store-key", "",
		"private key file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&newrg.StorePrefix, "store-prefix", "stolon/cluster",
		"the Stolon store base prefix")
}

func addRepGroup(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	err = commands.AddRepGroup(context.Background(), hl, cs, &cfg.StoreConnInfo, &newrg)
	if err != nil {
		hl.Fatalf("addrepgroup failed: %v", err)
	}
}
