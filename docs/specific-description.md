# Specific description ZPD service

- [Specific description ZPD service](#specific-description-zpd-service)
  - [1. Giới thiệu](#1-gi%e1%bb%9bi-thi%e1%bb%87u)
  - [2. Sử dụng package](#2-s%e1%bb%ad-d%e1%bb%a5ng-package)
  - [3. Protocol](#3-protocol)
  - [4. Các chức năng cung cấp](#4-c%c3%a1c-ch%e1%bb%a9c-n%c4%83ng-cung-c%e1%ba%a5p)
    - [4.1 Version 1.0.0](#41-version-100)
  - [5. Đặc tả](#5-%c4%90%e1%ba%b7c-t%e1%ba%a3)
    - [5.1 Public Service](#51-public-service)
      - [5.1.1 Đặc tả Data](#511-%c4%90%e1%ba%b7c-t%e1%ba%a3-data)
      - [5.1.2 Đặc tả API](#512-%c4%90%e1%ba%b7c-t%e1%ba%a3-api)
    - [5.2 Internal Service](#52-internal-service)
      - [5.2.1 Đặc tả Data](#521-%c4%90%e1%ba%b7c-t%e1%ba%a3-data)
      - [5.2.2 Đặc tả API](#522-%c4%90%e1%ba%b7c-t%e1%ba%a3-api)
  - [6. Thiết kế Key_Value](#6-thi%e1%ba%bft-k%e1%ba%bf-keyvalue)
    - [6.1 Database](#61-database)
    - [6.2 Table](#62-table)
    - [6.3 Row](#63-row)
    - [6.4 Index](#64-index)
  
## 1. Giới thiệu
Đây là tài liệu đặc tả về API cung cấp của ZPD service. Cũng như mô tả các gói tin gửi nhận giữa client và ZPD service.

## 2. Sử dụng package
Sử dụng và tham khảo các opensource sau:
- [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser)
- [cube2222/octosql](https://github.com/cube2222/octosql)
- [TiKV client](https://github.com/tikv/client-go)
- [parser TiDB](https://github.com/pingcap/parser)
  
## 3. Protocol
- ZPD sử dụng [gRPC](https://github.com/grpc/grpc-go) để xây dựng service. 
- Implement bằng ngôn ngữ [Go](https://golang.org/).
- Sử dụng [Consul](https://www.consul.io) để lựa chọn leader giữa các node và healthy check.

## 4. Các chức năng cung cấp
### 4.1 Version 1.0.0
- Tạo Connection tới ZPD.
- Close Connection.
- Create Database.
- Use Database.
- Drop Database.
- Show Database.
- Create Table.
- Show Table.
- Drop Table.
- Insert row.
- Select row (có index và không có index).
- Delete row (có index và không có index).

## 5. Đặc tả 
### 5.1 Public Service
#### 5.1.1 Đặc tả Data
***common.proto***
```proto

message Status {
  //code = 1 means success
  int32 code = 1;
  string error = 2;
}

message MessageResponse {
  Status status = 1;
}
```

***zpd.proto***

```proto
enum SQLType {
    DEFAULT = 0;
    SHOWDATABASE = 1;
    SHOWTABLE = 2;
    SELECT = 3;
}

message Databases {
    repeated string databases = 1;
}

message NameTables {
    repeated string nametables = 1;
}

message Item{
    int32 type = 1;
    bytes val = 2;
    bool bool = 3;
}

message Row {
    repeated Item items = 1;
}

message Rows {
    repeated Row rows = 1;
}

message Ping {
    int64 timestamp = 1;
}

message Pong{
    int64 timestamp = 1;
    string serviceName = 2;
    zpd.common.proto.Status status = 3;
}

message ConnectionDBRequest {
    string dbname = 1;
}

message CloseConnectionDBRequest {        
}

message StatementRequest{
    SQLType type = 1;
    string sql = 2;
}

message StatementResponse{
    SQLType type = 1;
    bytes data = 2;
    zpd.common.proto.Status status = 3;
}
```

#### 5.1.2 Đặc tả API
***zpd_api***

```proto
service ZPDService {
    // ping-pong
    rpc Ping (zpd.data.proto.Ping) returns (zpd.data.proto.Pong) {
    }
    //Connection db
    rpc ConnectDatabase(zpd.data.proto.ConnectionDBRequest) returns (zpd.common.proto.MessageResponse){
    }
    //Close connection db
    rpc CloseConnectionDatabase(zpd.data.proto.CloseConnectionDBRequest) returns (zpd.common.proto.MessageResponse){
    }
    //Statement api
    rpc ExecuteStatement(zpd.data.proto.StatementRequest) returns (zpd.data.proto.StatementResponse){
    }
}
```

### 5.2 Internal Service
#### 5.2.1 Đặc tả Data
***common_internal.proto***
```proto
message Status {
  //code = 1 means success
  int32 code = 1;
  string error = 2;
}

message MessageResponse {
  Status status = 1;
}
```

***zpd_internal.proto***

```proto
message Table {
}

message Schema {
    uint64 ID = 1;
    string dbname = 2;
    repeated Table tables = 3;
}

message SchemaRequest {
    string dbname = 1;
}

message SchemaResponse {
    Schema schema = 1;
    zpd_internal.common.proto.Status status = 2;
}

message CreateDatabaseRequest {
    string dbname = 1;
}

message CreateDatabaseResponse{
    zpd_internal.common.proto.Status status = 1;
}

message DropDatabaseRequest {
    string dbname = 1;
}

message DropDatabaseResponse{
    zpd_internal.common.proto.Status status = 1;
}

message GetDatabasesRequest{

}

message GetDatabasesResponse{
    repeated string databases = 1;
    zpd_internal.common.proto.Status status = 2;
}
```
#### 5.2.2 Đặc tả API
```proto
service ZPDInternalService {
    rpc GetSchema(zpd_internal.data.proto.SchemaRequest) returns (zpd_internal.data.proto.SchemaResponse){
    }

    rpc CreateDatabase(zpd_internal.data.proto.CreateDatabaseRequest) returns (zpd_internal.data.proto.CreateDatabaseResponse){
    }

    rpc DropDatabase(zpd_internal.data.proto.DropDatabaseRequest) returns (zpd_internal.data.proto.DropDatabaseResponse){
    }

    rpc GetDatabases(zpd_internal.data.proto.GetDatabasesRequest) returns (zpd_internal.data.proto.GetDatabasesResponse){
    }
}
```
Xem chi tiết các file đặc tả:
- [Internal API](../source/api/internal-proto)
- [Public API](../source/api/publicl-proto)

## 6. Thiết kế Key_Value
Tất cả key và value đều được chuyển thành bytes.

### 6.1 Database
**key_value cho database**

- Key: DB:ID 
- Value: object schema

`Note:` 1 <= ID <= max(Int64)

**key_value cho next ID Database**

- Key: nextDBID
- Value: number
- ID tăng dần, kiểu int64

### 6.2 Table
**key_value cho Table**

- Key: TB:DBID:ID
- Value: object table

`Note:` 1 <= ID <= max(Int64)

**key_value cho next ID Table**

- Key: nextTBID:DBID
- Value: number
- ID tăng dần, kiểu int64
  
### 6.3 Row
- Key: row:DBID:TBID:ID
- Value: object row

`Note:` 1 <= ID <= max(Int64)

### 6.4 Index
- Key: i:DBID:TBID:nameCol:value
- Value: rowID