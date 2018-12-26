// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/spf13/cobra"

	"postgrespro.ru/hodgepodge/internal/cluster"
	"postgrespro.ru/hodgepodge/internal/cluster/commands"
)

var specFile string

var initCmd = &cobra.Command{
	Use:   "init",
	Run:   initCluster,
	Short: "Initialize a new cluster",
}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.PersistentFlags().StringVarP(&specFile, "spec-file", "f", "",
		`json file containing the new cluster spec. If "-", read from stdin. The format and defaults are:
{
  // Postgres superuser auth method. Only trust, md5 and scram-sha-256 are supported.
  "PgSuAuthMethod": "trust",
  // Postgres superuser password.
  "PgSuPassword": "",
  // Postgres superuser user name. User name and its auth method must be the same at all replication groups. Default is current os user.
  "PgSuUsername": "joe"
  // Stolon spec as passed to 'stolonctl init --file'
  "StolonSpec": {
    ...
  }
}
`)
}

func initCluster(cmd *cobra.Command, args []string) {
	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		hl.Fatalf("failed to create store: %v", err)
	}
	defer cs.Close()

	if specFile != "" && len(args) == 1 {
		hl.Fatalf("cluster spec must be provided as direct argument or as file to read (--spec-file/-f option), but not both")
	}

	var specdata = []byte("{}")
	if specFile != "" {
		var err error
		if specFile == "-" {
			specdata, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				log.Fatalf("cannot read from stdin: %v", err)
			}
		} else {
			specdata, err = ioutil.ReadFile(specFile)
			if err != nil {
				log.Fatalf("cannot read file: %v", err)
			}
		}
	}
	if len(args) == 1 {
		specdata = []byte(args[0])
	}

	var spec cluster.ClusterSpec
	if err := json.Unmarshal(specdata, &spec); err != nil {
		log.Fatalf("failed to unmarshal cluster spec: %v", err)
	}
	err = commands.InitCluster(context.TODO(), cs, &spec)
	if err != nil {
		log.Fatalf("%v", err)
	}
}
