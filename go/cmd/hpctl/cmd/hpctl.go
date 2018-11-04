package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/store"
)

// Here we will store args
var cfg cmdcommon.CommonConfig

// check options
func CheckConfig(cfg *cmdcommon.CommonConfig) error {
	if cfg.ClusterName == "" {
		return fmt.Errorf("cluster name required")
	}

	return nil
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hpctl",
	Short: "hodgepodge command line client",
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

	rootCmd.PersistentFlags().StringVar(&cfg.StoreEndpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0], "a comma-delimited list of store endpoints (use https scheme for tls communication)")
	rootCmd.PersistentFlags().StringVar(&cfg.ClusterName, "cluster-name", "", "cluster name")
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
