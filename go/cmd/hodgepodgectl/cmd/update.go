// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
	"postgrespro.ru/hodgepodge/internal/cluster"
)

// store args here
type updateOptsT struct {
	patch bool
	file  string
}

var updateOpts updateOptsT

var updateSpecCmd = &cobra.Command{
	Use:   "update",
	Run:   update,
	Args:  cobra.MaximumNArgs(1),
	Short: "Update Stolon's spec at all repgroups and in the store (for new repgroups). This is not atomic, so should be retried in case of any failures.",
}

func init() {
	rootCmd.AddCommand(updateSpecCmd)
	updateSpecCmd.PersistentFlags().BoolVarP(&updateOpts.patch, "patch", "p", false, "patch the current cluster specification instead of replacing it")
	updateSpecCmd.PersistentFlags().StringVarP(&updateOpts.file, "file", "f", "", "file containing a complete cluster specification or a patch to apply to the current cluster specification. if '-', read from stdin")
}

func update(cmd *cobra.Command, args []string) {
	if updateOpts.file == "" && len(args) == 0 {
		die("no cluster spec provided as argument and no file provided (--file/-f option)")
	}
	if updateOpts.file != "" && len(args) == 1 {
		die("cluster spec must be provided as direct argument or as file to read (--file/-f option), but not both")
	}

	var data []byte
	if len(args) == 1 {
		data = []byte(args[0])
	} else {
		var err error
		if updateOpts.file == "-" {
			data, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				die("cannot read from stdin: %v", err)
			}
		} else {
			data, err = ioutil.ReadFile(updateOpts.file)
			if err != nil {
				die("cannot read file: %v", err)
			}
		}
	}

	cs, err := cluster.NewClusterStore(&cfg)
	if err != nil {
		die("failed to create store: %v", err)
	}
	defer cs.Close()
	err = cs.UpdateStolonSpec(context.TODO(), &cfg.StoreConnInfo, data, updateOpts.patch)
	if err != nil {
		die("failed to update the spec: %v", err)
	}
}
