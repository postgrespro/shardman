// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"os/user"

	"github.com/spf13/cobra"

	cmdcommon "postgrespro.ru/hodgepodge/cmd"
	"postgrespro.ru/hodgepodge/internal/cluster"
)

// init-specific options
type InitConfig struct {
	pgSuAuthMethod string
	pgSuPassword   string
	pgSuUsername   string
}

var initcfg InitConfig

var initCmd = &cobra.Command{
	Use:   "init",
	Run:   initCluster,
	Short: "Initialize a new cluster",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if initcfg.pgSuAuthMethod != "trust" && initcfg.pgSuPassword == "" {
			die("Password not provided and authmethod is not trust")
		}
	},
}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().StringVar(&initcfg.pgSuAuthMethod, "pg-su-auth-method",
		"scram-sha-256",
		"postgres superuser auth method. Only trust, md5 and scram-sha-256 are supported")
	initCmd.Flags().StringVar(&initcfg.pgSuPassword, "pg-su-password",
		"", "postgres superuser password")
	user, err := user.Current()
	if err != nil {
		panic(err)
	}
	initCmd.Flags().StringVar(&initcfg.pgSuUsername, "pg-su-username",
		user.Name, "postgres user name. User name and its auth method must be the same at all replication groups. Default is current os user.")

}

func initCluster(cmd *cobra.Command, args []string) {
	cs, err := cmdcommon.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()

	cldata, _, err := cs.GetClusterData(context.TODO())
	if err != nil {
		die("cannot get cluster data: %v", err)
	}
	if cldata != nil {
		stdout("WARNING: overriding existing cluster")
	}

	err = cs.PutTables(context.TODO(), []cluster.Table{})
	if err != nil {
		die("failed to save tables data in store")
	}
	err = cs.PutRepGroups(context.TODO(), map[int]*cluster.RepGroup{})
	if err != nil {
		die("failed to save repgroup data in store")
	}
	// We configure access for su from anywhere. TODO: allow more restrictive
	autopgrestart := true // apparently no way in go to get addr of literal
	stolonspec := &cluster.StolonSpec{
		PGHBA: []string{
			"host all " + initcfg.pgSuUsername + " 0.0.0.0/0 " + initcfg.pgSuAuthMethod,
			"host all " + initcfg.pgSuUsername + " ::0/0 " + initcfg.pgSuAuthMethod},
		PGParameters: map[string]string{
			"log_statement":             "all",
			"log_line_prefix":           "%m [%r][%p]",
			"log_min_messages":          "INFO",
			"max_prepared_transactions": "100",
			// FIXME: stolon forces hot_standby, apparently for its own conns
			// "hot_standby": "off", // disable connections to replicas
			"wal_level":                "logical", // rebalance
			"shared_preload_libraries": "hodgepodge",
		},
		AutomaticPgRestart: &autopgrestart,
	}
	cldatanew := &cluster.ClusterData{
		FormatVersion:  cluster.CurrentFormatVersion,
		PgSuAuthMethod: initcfg.pgSuAuthMethod,
		PgSuPassword:   initcfg.pgSuPassword,
		PgSuUsername:   initcfg.pgSuUsername,
		StolonSpec:     stolonspec,
	}
	err = cs.PutClusterData(context.TODO(), cldatanew)
	if err != nil {
		die("failed to save clusterdata in store")
	}

}
