mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
bin_dir := $(mkfile_dir)/bin

version := $(shell git rev-parse HEAD)
module_root := postgrespro.ru/hodgepodge
ld_flags := "-X $(module_root)/cmd.HodgepodgeVersion=$(version)"

all:
	echo $(ld_flags)
	if [ -x "$$(command -v goimports)" ]; then echo "fixing imports & formatting..."; goimports -w .; fi
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/hodgepodgectl  postgrespro.ru/hodgepodge/cmd/hodgepodgectl
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/hodgepodge-monitor postgrespro.ru/hodgepodge/cmd/monitor
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/hodgepodge-ladle postgrespro.ru/hodgepodge/cmd/ladle
	go build -ldflags $(ld_flags) -i -o $(bin_dir)/hodgepodge-bowl postgrespro.ru/hodgepodge/cmd/bowl
