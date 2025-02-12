#!/usr/bin/env bash

# If you don't have protoc-gen-go:
#     go install google.golang.org/protobuf/cmd/protoc-gen-go

PATH="$PATH:${GOPATH}/bin:${HOME}/go/bin" protoc --go_out=../hdfs/ ./*.proto
