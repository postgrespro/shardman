package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/store"
)

var cmdInit = &cobra.Command{
	Use:   "init",
	Run:   initCluster,
	Short: "Initialize a new cluster",
}

func init() {
	rootCmd.AddCommand(cmdInit)
}

func initCluster(cmd *cobra.Command, args []string) {
	// fmt.Printf("initting cluster %s\n", cfg.ClusterName)
	cs, err := store.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}

	rg, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("cannot get rep groups data: %v", err)
	}
	stderr("repgroups are: %v", rg)

	cs.Close()
}
