package cmd

import (
	"context"
	"log"
	"strings"

	"github.com/spf13/cobra"
	"postgrespro.ru/shardman/internal/ladle"
)

// init-specific options
var nodesString string

var addNodesCmd = &cobra.Command{
	Use:   "addnodes",
	Run:   addNodes,
	Short: "Add nodes to the cluster",
	Long: `Adding nodes to the cluster is a two-phase process: first ladle computes which
daemons should run on which nodes and pushes this to etcd. bowl daemon who
should be running on each target node reacts to this and starts those daemons.
Ladle then waits for stolon instances to emerge, connects to them and performs
initialization.  If something failes after nodes were registered in etcd, but
before stolon instances got initialized, fixrepgroups command can be used to
retry initialization.`,
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
