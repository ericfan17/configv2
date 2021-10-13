#!/bin/bash

go run main.go \
  -csi.config.repo.root="$GOPATH/src/github.com/17media/configs/envs/dev"\
  -csi.config.etcd.root="/configs/envs/dev" \
  -csi.config.etcd.machines="http://127.0.0.1:12379"
