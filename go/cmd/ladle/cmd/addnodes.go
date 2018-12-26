package cmd

import (
	"context"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/ladle"
)

// init-specific options
var nodesString string

var addNodesCmd = &cobra.Command{
	Use:   "addnodes",
	Run:   addNodes,
	Short: "Add nodes to the cluster",
}

func init() {
	rootCmd.AddCommand(addNodesCmd)

	addNodesCmd.PersistentFlags().StringVarP(&nodesString, "nodes", "n", "", "comma-separated list of hostnames of nodes to add")
}

func addNodes(cmd *cobra.Command, args []string) {
	if nodesString == "" {
		log.Fatalf("No nodes specified")
	}
	nodes := strings.Split(nodesString, ",")

	ls, err := ladle.NewLadleStore(&cfg)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer ls.Close()

	err = ls.AddNodes(context.Background(), hl, nodes)
	if err != nil {
		log.Fatalf("failed to add nodes: %v", err)
	}
}
