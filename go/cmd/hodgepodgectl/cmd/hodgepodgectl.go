// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"fmt"
	"log"
	"os"

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
	Use:     "hodgepodgectl",
	Version: cmdcommon.HodgepodgeVersion,
	Short:   "hodgepodge command line client. Note: you must always run at most one instance of hodgepodgectl at time.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		hl = hplog.GetLoggerWithLevel(logLevel)

		if err := cmdcommon.CheckConfig(&cfg); err != nil {
			hl.Fatalf(err.Error())
		}
	},
	// bare command does nothing
}

// Entry point
func Execute() {
	if err := utils.SetFlagsFromEnv(rootCmd.PersistentFlags(), "HPCTL"); err != nil {
		log.Fatalf("%v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Executed on package init
func init() {
	cmdcommon.AddCommonFlags(rootCmd, &cfg, &logLevel)
}
