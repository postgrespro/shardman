package cmd

import (
	"context"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"postgrespro.ru/shardman/internal/ladle"
)

// init-specific options
var rmNodesString string

var rmNodesCmd = &cobra.Command{
	Use:   "rmnodes",
	Run:   rmNodes,
	Short: "Remove nodes from the cluster. Removes nodes from metadata and unregisters corresponding repgroups. Note that tn case of clover placement policy, the whole clovers containing given nodes will be removed, meaning that additional nodes might get removed.",
}

func init() {
	rootCmd.AddCommand(rmNodesCmd)

	rmNodesCmd.PersistentFlags().StringVarP(&rmNodesString, "nodes", "n", "", "comma-separated list of hostnames of nodes to remove")
}

func rmNodes(cmd *cobra.Command, args []string) {
	if rmNodesString == "" {
		log.Fatalf("No nodes specified")
	}
	nodes := strings.Split(rmNodesString, ",")

	ls, err := ladle.NewLadleStore(&cfg)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer ls.Close()

	err = ls.RmNodes(context.Background(), hl, nodes)
	if err != nil {
		log.Fatalf("failed to remove nodes: %v", err)
	}
}
