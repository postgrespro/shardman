package cmd

import (
	"context"
	"fmt"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/pg"
	"postgrespro.ru/hodgepodge/internal/store"
)

// we will store args directly in RepGroup struct
var newrg cluster.RepGroup

var addrgCmd = &cobra.Command{
	Use:   "addrepgroup",
	Run:   addRepGroup,
	Short: "Add replication group to the cluster",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		if err := CheckConfig(&cfg); err != nil {
			die(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(addrgCmd)

	addrgCmd.Flags().StringVar(&newrg.StolonName, "stolon-name", "",
		"cluster-name of Stolon instance being added. Must be unique for the whole hodgepodge cluster")
	addrgCmd.Flags().StringVar(&newrg.StoreEndpoints, "store-endpoints",
		store.DefaultEtcdEndpoints[0],
		"a comma-delimited list of Stolon store endpoints (use https scheme for tls communication)")
	addrgCmd.Flags().StringVar(&newrg.StoreCAFile, "store-ca-file", "",
		"verify certificates of HTTPS-enabled Stolon store servers using this CA bundle")
	addrgCmd.Flags().StringVar(&newrg.StoreCertFile, "store-cert-file", "",
		"certificate file for client identification to the Stolon store")
	addrgCmd.Flags().StringVar(&newrg.StoreKey, "store-key", "",
		"private key file for client identification to the Stolon store")
	addrgCmd.Flags().BoolVar(&newrg.StoreSkipTLSVerify, "store-skip-tls-verify",
		false, "skip Stolon store certificate verification")
	addrgCmd.Flags().StringVar(&newrg.StorePrefix, "store-prefix", "stolon/cluster",
		"the Stolon store base prefix")
}

func addRepGroup(cmd *cobra.Command, args []string) {
	cs, err := store.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()

	cldata, _, err := cs.GetClusterData(context.TODO())
	if err != nil {
		die("cannot get cluster data: %v", err)
	}
	if cldata == nil {
		die("cluster %v not found", cfg.ClusterName)
	}

	// TODO: wait until config is actually applied
	err = store.StolonUpdate(&newrg, true, cldata.StolonSpec)
	if err != nil {
		die(err.Error())
	}

	ss, err := store.NewStolonStore(&newrg)
	if err != nil {
		die("failed to create Stolon store: %v", err)
	}
	defer ss.Close()

	master, err := ss.GetMaster(context.TODO())
	if err != nil {
		die("failed to get Stolon master: %v", err)
	}
	if master == nil {
		die("stolon's clusterdata not found")
	}

	connconfig, err := pgx.ParseConnectionString(pg.FormSuConnstr(master, cldata))
	if err != nil {
		die("connstring parsing failed") // should not happen
	}
	conn, err := pgx.Connect(connconfig)
	if err != nil {
		die("Unable to connect to database: %v", err)
	}
	defer conn.Close()

	_, err = conn.Exec("create extension if not exists hodgepodge cascade")
	if err != nil {
		die("Unable to create extension: %v", err)
	}
	err = conn.QueryRow("select system_identifier from pg_control_system()").Scan(&newrg.SystemId)
	if err != nil {
		die("Failed to retrieve sysid: %v", err)
	}

	tables, _, err := cs.GetTables(context.TODO())
	if err != nil {
		die("Failed to get tables from store: %v", err)
	}
	for _, table := range tables {
		_, err = conn.Exec(table.Sql)
		if err != nil {
			die("Failed to create table: %v", err)
		}
	}

	rgs, _, err := cs.GetRepGroups(context.TODO())
	if err != nil {
		die("Failed to get repgroups: %v", err)
	}
	var newrgid int = 0
	for rgid, rg := range rgs {
		if rg.SystemId == newrg.SystemId {
			die("Repgroup with sys id %v already exists", rg.SystemId)
		}
		if rgid > newrgid {
			newrgid = rgid
		}
	}
	newrgid++
	rgs[newrgid] = &newrg

	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	bcst.Push(newrgid, "fewfew")
	bcst.Push(newrgid, "select 2::text")
	res, err := bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}
	fmt.Printf("res is %v", res)

	// _, err = conn.Exec("drop table if exists dd")
	// _, err = conn.Exec("create table t (i int)")
	// if err != nil {
	// die(err.Error())
	// }
	// _, err = conn.Exec("insert into tt values (10)")
	// rows, err := conn.Query("prepare transaction 'fewfwe'")
	// if err != nil {
	// die("Unable to create table dd: %v", err)
	// }

	// for rows.Next() {
	// 	var n int32
	// 	err = rows.Scan(&n)
	// 	fmt.Printf(string(n))
	// }
	// if rows.Err() != nil {
	// 	die("rows err failed", rows.Err().Error())
	// }
	// _, err = conn.Exec("insert into t values (10)")
	// if err != nil {
	// die("insert err %v", err)
	// }
	// err = tx.Rollback()
	// if err != nil {
	// die("ahaha %v", err.Error())
	// }
}
