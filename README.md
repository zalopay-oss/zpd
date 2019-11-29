# ZPD service

<div align="center">
  <img src="./images/model_overview_zpd.png" width=500>
</div>

- [ZPD service](#zpd-service)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Requirement](#requirement)
  - [Install](#install)
  - [Build](#build)
  - [Run](#run)
  - [Test](#test)
  - [Document](#document)
  - [Contribution](#contribution)
  - [Acknowledgments](#acknowledgments)

## Overview
ZPD service là một challenge thử việc của bạn [AJPham](https://github.com/phamtai97). ZPD service đóng vai trò nhận các yêu cầu SQL từ phía client gửi lên, sau đó parse câu SQL thành Abstract syntax tree (AST), mapping câu SQL thành dạng key-value và cuối cùng là thực thi data key-value xuống tầng storage. ZPD service kết hợp với [Consul](https://www.consul.io/) làm tính năng bình chọn leader giữa các node ZPD trong cluster. Phần storage ZPD service sử dụng [TiKV](https://github.com/tikv/tikv) để lưu trữ key-value và [PD](https://github.com/pingcap/pd) để quản lí và tương tác với TiKV. Trong phạm vi của project thì ZPD service chỉ có thể thực hiện được một số câu SQL đơn giản.

ZPD service được implement bằng ngôn ngữ Go. Sử dụng [gRPC](https://github.com/grpc/grpc-go) để build protocol và service. ZPD hoạt động theo flow sau:

<div align="center">
  <img src="./images/flow-ZPD.png">
</div>


## Architecture
Kiến trúc của ZPD service:

<div align="center">
  <img src="./images/zpd_layer.png" width="250">
</div>

Gồm các layer:
- Connection layer
- Parser layer
- Core layer: 
  - Executor
  - Consul Agent
  - Bridge API
- Data access layer
- Storage:
  - TiKV client layer

Xem chi tiết phần kiến trúc ZPD service [ở đây](./docs/architecture.md).

## Requirement
- Golang version >= 1.12
- gRPC
- Docker version >= 17.06 và Docker Compose 
- Locust

## Install
- Install [Golang](https://golang.org/doc/install)
- Install [gRPC](https://grpc.io/docs/quickstart/go/)
- Install [Docker](https://docs.docker.com/get-started/)
- Install [Locust](https://locust.io/)
- Install TiKV và PD sử dụng [Docker Compose](https://tikv.org/docs/3.0/tasks/deploy/docker-compose/)
- Install Consul bằng [Docker](https://hub.docker.com/_/consul).

## Build
Clone project từ gitlab về máy tính.

```sh
# Clone
$ git clone https://gitlab.zalopay.vn/zpx-core-team/tidb-internals.git
```
## Run
ZPD service, PD, TiKV, Consul đều được build bằng Docker compose. Chỉ cần đi đến thư mục docker-compose và chạy docker-compose up.

```sh
# Đi đến thư mục dockrer-compose
$ cd ./tidb-layer/source/docker-compose

# Run docker-compose
$ docker-compose up
```

## Test
Phải chạy ZPD service trước như ở phần [Run](#run). Sau đó test các flow API mà ZPD cung cấp như sau:

```sh
# Đi đến thư mục cmd/client
$ cd ./tidb-layer/source/cmd/client

# Chạy các bài test
$ go test -run TestClientExecuteCreateDB 
```
- Có thể viết thêm các unit test ở file client_test.go như format của các unit test có sẵn.

## Document
Các document khác về ZPD service có thể đọc ở:
  - [Specific description](./docs/specific-description.md)
  - [Sequence diagram](./docs/sequence-diagram.md)
  - [Architecture ZPD service](./docs/architecture.md)
  - [Overview ZPD](docs/overview-ZPD.md)
  - [Handle DDL in ZPD](docs/handle-ddl.md)

## Contribution
Project được xây dựng bởi hai bạn [AJPham](https://github.com/phamtai97) và bạn [Alex Nguyen](https://github.com/quocanh1897) dưới sự hướng dẫn từ anh [Anh Le (Andy)](https://github.com/anhldbk).

## Acknowledgments
- Project có sử dụng các bên thứ ba như:
  - [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser): một open source về parser SQL khá tốt.
  - [Consul](https://github.com/hashicorp/consul): dùng để quản lý việc chọn leader và healthy check.
  - [PingCap](https://github.com/pingcap): hỗ trợ phần PD và TiKV.