mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
bin_dir := $(mkfile_dir)/bin

version := $(shell git rev-parse HEAD)
module_root := postgrespro.ru/shardman
ld_flags := "-X $(module_root)/cmd.ShardmanVersion=$(version)"

# go linker 1.11 has high memory consumption, lowering num of parallel
# invocations might alleviate this
ifdef BUILD_P
build_flags += -p $(BUILD_P)
endif

all:
	echo $(ld_flags)
	if [ -x "$$(command -v goimports)" ]; then echo "fixing imports & formatting..."; goimports -w .; fi
	go build $(build_flags) -ldflags $(ld_flags) -i -o $(bin_dir)/shardmanctl  postgrespro.ru/shardman/cmd/shardmanctl
	go build $(build_flags) -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-monitor postgrespro.ru/shardman/cmd/monitor
	go build $(build_flags) -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-ladle postgrespro.ru/shardman/cmd/ladle
	go build $(build_flags) -ldflags $(ld_flags) -i -o $(bin_dir)/shardman-bowl postgrespro.ru/shardman/cmd/bowl

clean:
	rm $(bin_dir)/*
