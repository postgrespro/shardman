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

	connstr, err := pg.GetSuConnstr(&newrg, cldata)
	if err != nil {
		die("Couldn't get connstr: %v", err)
	}

	connconfig, err := pgx.ParseConnectionString(connstr)
	if err != nil {
		die("connstring parsing \"%s\" failed: %v", connstr, err) // should not happen
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

	// TODO: wait until config is actually applied
	err = store.StolonUpdate(&newrg, newrgid, true, cldata.StolonSpec)
	if err != nil {
		die(err.Error())
	}

	bcst, err := pg.NewBroadcaster(rgs, cldata)
	if err != nil {
		die("Failed to create broadcaster: %v", err)
	}
	defer bcst.Close()

	bcst.Begin()
	// create foreign servers to all rgs at newrg and vice versa
	newrgconnstrmap, err := store.GetSuConnstrMap(context.TODO(), &newrg, cldata)
	newrgumopts := pg.FormUserMappingOpts(newrgconnstrmap)
	newrgfsopts := pg.FormForeignServerOpts(newrgconnstrmap)
	if err != nil {
		die("Failed to get new rg connstr")
	}
	for rgid, rg := range rgs {
		if rgid == newrgid {
			continue
		}
		rgconnstrmap, err := store.GetSuConnstrMap(context.TODO(), rg, cldata)
		if err != nil {
			die("Failed to get rg %s connstr", rg.StolonName)
		}
		rgumopts := pg.FormUserMappingOpts(rgconnstrmap)
		rgfsopts := pg.FormForeignServerOpts(rgconnstrmap)
		bcst.Push(newrgid, fmt.Sprintf("drop server if exists hp_rg_%d cascade", rgid))
		bcst.Push(newrgid, fmt.Sprintf("create server hp_rg_%d foreign data wrapper postgres_fdw %s", rgid, rgfsopts))
		bcst.Push(rgid, fmt.Sprintf("drop server if exists hp_rg_%d cascade", newrgid))
		bcst.Push(rgid, fmt.Sprintf("create server hp_rg_%d foreign data wrapper postgres_fdw %s", newrgid, newrgfsopts))
		bcst.Push(newrgid, fmt.Sprintf("drop user mapping if exists for current_user server hp_rg_%d", rgid))
		bcst.Push(newrgid, fmt.Sprintf("create user mapping for current_user server hp_rg_%d %s", rgid, rgumopts))
		bcst.Push(rgid, fmt.Sprintf("drop user mapping if exists for current_user server hp_rg_%d", newrgid))
		bcst.Push(rgid, fmt.Sprintf("create user mapping for current_user server hp_rg_%d %s", newrgid, newrgumopts))
	}
	// create tables and foreign partitions
	tables, _, err := cs.GetTables(context.TODO())
	if err != nil {
		die("Failed to get tables from store: %v", err)
	}
	for _, table := range tables {
		bcst.Push(newrgid, table.Sql)
		for pnum := 0; pnum < table.Nparts; pnum++ {
			bcst.Push(newrgid, fmt.Sprintf("create foreign table %s partition of %s for values with (modulus %d, remainder %d) server hp_rg_%d options (table_name %s)",
				pg.QI(fmt.Sprintf("%s_%d_fdw", table.Relname, pnum)),
				pg.QI(table.Relname),
				table.Nparts,
				pnum,
				table.Partmap[pnum],
				pg.QL(fmt.Sprintf("%s_%d", table.Relname, pnum))))
		}
	}
	_, err = bcst.Commit(true)
	if err != nil {
		die("bcst failed: %v", err)
	}

	// TODO make atomic
	err = cs.PutRepGroups(context.TODO(), rgs)
	if err != nil {
		die("failed to save repgroup data in store: %v", err)
	}
}
