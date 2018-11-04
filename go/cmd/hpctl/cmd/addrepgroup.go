package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/store"
	"postgrespro.ru/hodgepodge/internal/store/stolonstore"
)

// we will store args directly in RepGroup struct
var rg cluster.RepGroup

var addrgCmd = &cobra.Command{
	Use:   "addrepgroup",
	Run:   addRepGroup,
	Short: "Add replication group to the cluster",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(addrgCmd)

	addrgCmd.Flags().StringVar(&rg.StolonName, "stolon-name", "",
		"cluster-name of Stolon instance being added")
	addrgCmd.Flags().StringVar(&rg.StoreEndpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0],
		"a comma-delimited list of Stolon store endpoints (use https scheme for tls communication)")
	addrgCmd.Flags().StringVar(&rg.StoreCAFile, "store-ca-file", "",
		"verify certificates of HTTPS-enabled Stolon store servers using this CA bundle")
	addrgCmd.Flags().StringVar(&rg.StoreCertFile, "store-cert-file", "",
		"certificate file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&rg.StoreKey, "store-key", "",
		"private key file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&rg.StorePrefix, "store-prefix", "stolon/cluster",
		"the Stolon store base prefix")
}

func addRepGroup(cmd *cobra.Command, args []string) {
	ss, err := stolonstore.NewStolonStore(&rg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer ss.Close()
	master, err := ss.GetMaster(context.TODO())
	if err != nil {
		die("failed to get master: %v", err)
	}
	if master == nil {
		die("clusterdata not found")
	}
	fmt.Printf("master %v", master)
}
