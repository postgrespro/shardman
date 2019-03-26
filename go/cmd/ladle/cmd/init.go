// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/spf13/cobra"

	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/ladle"
)

// init-specific options
var specFile string

type initConfig struct {
	LadleSpec   ladle.LadleSpec
	ClusterSpec cluster.ClusterSpec
}

var initCfg initConfig

var initCmd = &cobra.Command{
	Use:   "init",
	Run:   initCluster,
	Short: "Initialize a new cluster",
}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.PersistentFlags().StringVarP(&specFile, "spec-file", "f", "", "file containing the new cluster spec")
}

func initCluster(cmd *cobra.Command, args []string) {
	if specFile != "" && len(args) == 1 {
		log.Fatalf("cluster spec must be provided as direct argument or as file to read (--spec-file/-f option), but not both")
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

	var initCfg initConfig
	if err := json.Unmarshal(specdata, &initCfg); err != nil {
		log.Fatalf("failed to unmarshal spec: %v", err)
	}

	ls, err := ladle.NewLadleStore(&cfg)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer ls.Close()
	err = ls.InitCluster(context.Background(), hl, &initCfg.LadleSpec, &initCfg.ClusterSpec, &cfg)
	if err != nil {
		log.Fatalf("failed to init cluster: %v", err)
	}
}
