// Copyright (c) 2018, Postgres Professional

package commands

import (
	"context"
	"fmt"
	"log"
	"os/user"

	"postgrespro.ru/hodgepodge/internal/cluster"
)

// scribbles directly on input
func adjustSpecDefaults(spec *cluster.ClusterSpec) {
	if spec.PgReplAuthMethod == "" {
		spec.PgReplAuthMethod = "trust"
	}
	if spec.PgReplUsername == "" {
		spec.PgReplUsername = "repluser"
	}

	if spec.PgSuAuthMethod == "" {
		spec.PgSuAuthMethod = "trust"
	}
	if spec.PgSuUsername == "" {
		u, _ := user.Current()
		spec.PgSuUsername = u.Username
	}

	// Other choices doesn't make sense at least for now
	var initMode cluster.ClusterInitMode = "new"
	spec.StolonSpec.InitMode = &initMode

	// If hba is not specified, configure access for su user from anywhere
	if spec.StolonSpec.PGHBA == nil {
		spec.StolonSpec.PGHBA = []string{
			"host all " + spec.PgSuUsername + " 0.0.0.0/0 " + spec.PgSuAuthMethod,
			"host all " + spec.PgSuUsername + " ::0/0 " + spec.PgSuAuthMethod}
	}
	// we definitely need this
	if spec.StolonSpec.AutomaticPgRestart == nil {
		autopgrestart := true // apparently no way in go to get addr of literal
		spec.StolonSpec.AutomaticPgRestart = &autopgrestart
	}
	// also set some reasonable pg confs, if not yet
	if spec.StolonSpec.PGParameters == nil {
		spec.StolonSpec.PGParameters = make(map[string]string)
	}
	pgConfDefaults := map[string]string{
		"log_statement":             "all",
		"log_line_prefix":           "%m [%r][%p]",
		"log_min_messages":          "INFO",
		"max_prepared_transactions": "100",
		"wal_level":                 "logical", // rebalance
		"shared_preload_libraries":  "hodgepodge",
	}
	// impose given config over our defaults
	pgConfGiven := spec.StolonSpec.PGParameters
	spec.StolonSpec.PGParameters = pgConfDefaults
	for name, value := range pgConfGiven {
		spec.StolonSpec.PGParameters[name] = value
	}
}

func validateSpec(spec *cluster.ClusterSpec) error {
	if spec.PgReplAuthMethod != "trust" && spec.PgReplPassword == "" {
		return fmt.Errorf("Password not provided for password repl auth method")
	}
	if spec.PgSuAuthMethod != "trust" && spec.PgSuPassword == "" {
		return fmt.Errorf("Password not provided for password su auth method")
	}
	return nil
}

func InitCluster(ctx context.Context, cs *cluster.ClusterStore, spec *cluster.ClusterSpec) error {
	// fill defaults and validate config
	adjustSpecDefaults(spec)
	if err := validateSpec(spec); err != nil {
		return fmt.Errorf("spec validation failed: %v", err)
	}

	cldata, _, err := cs.GetClusterData(ctx)
	if err != nil {
		return fmt.Errorf("cannot get cluster data: %v", err)
	}
	if cldata != nil {
		log.Printf("WARNING: overriding existing cluster")
	}

	err = cs.PutRepGroups(ctx, map[int]*cluster.RepGroup{})
	if err != nil {
		return fmt.Errorf("failed to save repgroup data in store")
	}
	cldatanew := &cluster.ClusterData{
		FormatVersion: cluster.CurrentFormatVersion,
		Spec:          *spec,
	}
	err = cs.PutClusterData(ctx, cldatanew)
	if err != nil {
		return fmt.Errorf("failed to save clusterdata in store: %v", err)
	}

	return nil
}
