mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))
bin_dir := $(mkfile_dir)/bin

all:
	goimports -w .
	go build -i -o $(bin_dir)/hodgepodgectl  postgrespro.ru/hodgepodge/cmd/hodgepodgectl
	go build -i -o $(bin_dir)/hodgepodge-monitor postgrespro.ru/hodgepodge/cmd/monitor
