package cmd

import (
	"context"
	"os/user"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/store"
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
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().StringVar(&initcfg.pgSuAuthMethod, "pg-su-auth-method",
		"scram-sha-256",
		"postgres superuser auth method")
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
	// fmt.Printf("initting cluster %s\n", cfg.ClusterName)
	cs, err := store.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()

	rg, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("cannot get rep groups data: %v", err)
	}
	if rg != nil {
		stdout("WARNING: overriding existing cluster")
	}

	err = cs.PutTables(context.TODO(), []cluster.Table{})
	if err != nil {
		die("failed to save tables data in store")
	}
	err = cs.PutMasters(context.TODO(), map[int]*cluster.Master{})
	if err != nil {
		die("failed to save masters data in store")
	}
	err = cs.PutRepGroups(context.TODO(), map[int]*cluster.RepGroup{})
	if err != nil {
		die("failed to save repgroup data in store")
	}
	cldata := cluster.ClusterData{
		FormatVersion:  cluster.CurrentFormatVersion,
		PgSuAuthMethod: initcfg.pgSuAuthMethod,
		PgSuPassword:   initcfg.pgSuPassword,
		PgSuUsername:   initcfg.pgSuUsername,
	}
	err = cs.PutClusterData(context.TODO(), cldata)
	if err != nil {
		die("failed to save clusterdata in store")
	}

}
