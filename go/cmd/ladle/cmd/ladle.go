// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"log"

	"github.com/spf13/cobra"
	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/hplog"
	"postgrespro.ru/hodgepodge/internal/utils"
)

// Here we will store args
var cfg cluster.ClusterStoreConnInfo
var logLevel string

var hl *hplog.Logger

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "hodgepodge-ladle",
	Version: cmdcommon.HodgepodgeVersion,
	Short:   "deployment tool for hodgepodge",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		hl = hplog.GetLoggerWithLevel(logLevel)

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
