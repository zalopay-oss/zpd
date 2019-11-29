#!/bin/bash
protoc -I ../api/internal-proto/ ../api/internal-proto/*.proto --go_out=plugins=grpc:../pkg/internal-api