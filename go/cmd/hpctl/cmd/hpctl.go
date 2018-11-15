// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	cmdcommon "postgrespro.ru/hodgepodge/cmd"
)

// Here we will store args
var cfg cmdcommon.CommonConfig

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hpctl",
	Short: "hodgepodge command line client. Note: you must always run at most one instance of hpctl at time.",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := cmdcommon.CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
	},
	// bare command does nothing
}

// Entry point
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Create and register 'version' command
var cmdVersion = &cobra.Command{
	Use:   "version",
	Short: "Display the version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("hpctl v0.0.1")
	},
}

// Executed on package init
func init() {
	rootCmd.AddCommand(cmdVersion)

	cmdcommon.AddCommonFlags(rootCmd, &cfg)
}

// Copied from Stolon, apparently Go doesn't have this in lib
func stderr(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)
	fmt.Fprintln(os.Stderr, strings.TrimSuffix(out, "\n"))
}
func stdout(format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)
	fmt.Fprintln(os.Stdout, strings.TrimSuffix(out, "\n"))
}
func die(format string, a ...interface{}) {
	stderr(format, a...)
	os.Exit(1)
}
