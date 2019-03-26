mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
bin_dir := $(mkfile_dir)/bin

version := $(shell git rev-parse HEAD)
module_root := postgrespro.ru/shardman
ld_flags := "-X $(module_root)/cmd.ShardmanVersion=$(version)"

all:
	echo $(ld_flags)
	if [ -x "$$(command -v goimports)" ]; then echo "fixing imports & formatting..."; goimports -w .; fi
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/shardmanctl  postgrespro.ru/shardman/cmd/shardmanctl
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-monitor postgrespro.ru/shardman/cmd/monitor
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-ladle postgrespro.ru/shardman/cmd/ladle
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-bowl postgrespro.ru/shardman/cmd/bowl
