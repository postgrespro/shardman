// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"log"

	"github.com/spf13/cobra"
	cmdcommon "postgrespro.ru/shardman/cmd"
	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/shmnlog"
	"postgrespro.ru/shardman/internal/utils"
)

// Here we will store args
var cfg cluster.ClusterStoreConnInfo
var logLevel string

var hl *shmnlog.Logger

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "shardman-ladle",
	Version: cmdcommon.ShardmanVersion,
	Short:   "deployment tool for shardman",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		hl = shmnlog.GetLoggerWithLevel(logLevel)

		if err := cmdcommon.CheckConfig(&cfg); err != nil {
			hl.Fatalf("%v", err)
		}
	},
	// bare command does nothing
}

// Entry point
func Execute() {
	if err := utils.SetFlagsFromEnv(rootCmd.PersistentFlags(), "HPLADLE"); err != nil {
		log.Fatalf("%v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%v", err)
	}
}

// Executed on package init
func init() {
	cmdcommon.AddCommonFlags(rootCmd, &cfg, &logLevel)
}
