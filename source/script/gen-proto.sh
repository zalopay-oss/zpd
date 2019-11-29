#!/bin/bash
protoc -I ../api/public-proto/ ../api/public-proto/*.proto --go_out=plugins=grpc:../pkg/public-api