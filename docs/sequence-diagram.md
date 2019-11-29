# Sequence Diagrams
- [Sequence Diagrams](#sequence-diagrams)
  - [1. Giới thiệu](#1-gi%e1%bb%9bi-thi%e1%bb%87u)
  - [2. Sequence diagrams work flow](#2-sequence-diagrams-work-flow)
    - [2.1 API Ping](#21-api-ping)
    - [2.2 API ConnectDatabase](#22-api-connectdatabase)
    - [2.3 API CloseConnectionDatabase](#23-api-closeconnectiondatabase)
    - [2.4 API ExecuteStatement](#24-api-executestatement)
      - [2.4.1 Create Database](#241-create-database)
      - [2.4.2 Use Database](#242-use-database)
      - [2.4.3 Drop Database](#243-drop-database)
      - [2.4.5 Show Databases](#245-show-databases)
      - [2.4.6 Create Table](#246-create-table)
      - [2.4.7 Drop Table](#247-drop-table)
      - [2.4.8 Show Table of Database](#248-show-table-of-database)
      - [2.4.9 Insert Row](#249-insert-row)
      - [2.4.10 Select Row](#2410-select-row)
      - [2.4.11 Delete Row](#2411-delete-row)


## 1. Giới thiệu
Tài liệu sẽ trình bày về các `sequence diagrams` work flow API của ZPD cung cấp. Đưa ra cái nhìn tổng quan step by step trong work flow API đó. Sử dụng tài liệu này kết hợp với tài liệu [specific-description](./specific-description.md).

## 2. Sequence diagrams work flow
### 2.1 API Ping

```plantuml
title Ping-Pong ZPD

Client->ZPD: ping
ZPD-->Client: Pong
```

### 2.2 API ConnectDatabase

```plantuml
title Connect ZPD

Client->ZPD: connect ZPD
ZPD->TiPD: connect TiPD
alt connect success
TiPD-->ZPD: success
ZPD-->ZPD: create session
alt namedb != "*"
ZPD->TiKV: check schema 
alt schema exists
TiKV-->ZPD: schema
ZPD-->Client: response connect success with database
else schema does not exists
TiKV-->ZPD: error
ZPD-->Client: response connect success, but database does not exists
end
end
else connect fail   
TiPD-->ZPD: fail
ZPD-->Client: response connect fail
end
```

### 2.3 API CloseConnectionDatabase

```plantuml
title Close Connection ZPD

Client->ZPD: close conneciton
ZPD->TiPD: close connection
TiPD-->ZPD: success/fail
ZPD-->Client: response
```

### 2.4 API ExecuteStatement
#### 2.4.1 Create Database
- SQL:
```sql
CREATE DATABASE db_0;
```

- TH1 ZPD là node leader
```plantuml
title Create Database

Client->ZPD: create Database
ZPD->Consul: check leader
Consul-->ZPD: true
ZPD->TiKV: check database exists
alt database exists
TiKV-->ZPD: Yes
ZPD-->Client: response database exists
else database does not exists
TiKV-->ZPD: No
ZPD-->ZPD: prepare (key, value)
ZPD->TiKV: put (key, value)
alt put key-value pair success
TiKV-->ZPD: success
ZPD-->Client: response create database success
else put key-value pair fail
TiKV-->ZPD: fail
ZPD-->Client: response create database fail
end
end
```

- TH2 ZPD không phải node leader
```plantuml
title Create Database

Client->ZPD: create Database
ZPD->Consul: check leader
Consul-->ZPD: false
ZPD->Consul: get infomation current leader
Consul-->ZPD: infomation current leader
ZPD->ZPDLeader: create connection
alt if creating connection success
ZPDLeader-->ZPD: success
ZPD->ZPDLeader: call internal API Create Database
ZPDLeader-->ZPDLeader: handle API
alt handle success
ZPDLeader-->ZPD: response create database success
ZPD-->Client: response create database success
else handle fail
ZPDLeader-->ZPD: response create database fail
ZPD-->Client: response create database fail
end
else if creating connection fail
ZPDLeader-->ZPD: fail
ZPD-->Client: response create database fail
end
```

#### 2.4.2 Use Database
- SQL:
```sql
USE db_0;
```

- TH1 ZPD là node leader
```plantuml
title  Use Database

Client->ZPD: use Database
ZPD->Consul: check leader
Consul-->ZPD: true
ZPD->TiKV: get Schema success
alt schema exists
TiKV-->ZPD: Yes
ZPD-->ZPD: update schema for session
ZPD-->Client: response success
else get schema fail
TiKV-->ZPD: No
ZPD-->Client: response fail
end
```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.


#### 2.4.3 Drop Database
- SQL:
```sql
DROP DATABASE db_0;
```

- TH1 ZPD là node leader

```plantuml
title: Drop Database

Client-->ZPD: drop Database
ZPD->Consul: check leader
Consul-->ZPD: true
ZPD->TiKV: get Schemas
alt schema exists
TiKV-->ZPD: Yes
ZPD-->ZPD: gen Key
ZPD->TiKV: Delete database by key
TiKV-->ZPD: response
ZPD-->Client: response
else get Schema fail
TiKV-->ZPD: No
ZPD-->Client: response fail
end
```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.

`Note:` Update drop table của database (xử lý async) sau khi drop database.

#### 2.4.5 Show Databases
- SQL:
```sql
SHOW DATABASES;
```

- TH1 ZPD là node leader

```plantuml
title: Show Database

Client->ZPD: show Database
ZPD->Consul: check leader
Consul->ZPD: true
ZPD->TiKV: get databases
alt get databases success
TiKV-->ZPD: sucess
ZPD-->ZPD: prepare result
ZPD-->Client: response
else get database fail
TiKV-->ZPD: fail
ZPD-->Client: response fail
end
```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.

#### 2.4.6 Create Table
- SQL:
```sql
CREATE TABLE `table_0` (`id` int(200) NOT NULL, `name` varchar (10), PRIMARY KEY (`id`));

CREATE TABLE `table_0` (`id` int(200), `name` varchar (10), PRIMARY KEY (`id`), INDEX test_index(`name`));

CREATE TABLE `table_0` (`id` int(200), `name` varchar (10), `gmail` varchar(10), PRIMARY KEY (`id`), INDEX `test_index`(`name`, `gmail`));

CREATE TABLE `table_0` (`id` int(200), `name` varchar (10), `gmail` varchar(10), PRIMARY KEY (`id`), INDEX `test_index`(`name`), INDEX `test_index_0` (`gmail`));
```

- TH1 ZPD là node leader
```plantuml
title Create Table

Client->ZPD: create Table
ZPD-->ZPD: check the database being used by the session
alt database is null
ZPD-->Client: do not connect database
else database is not null
ZPD->Consul: check leader
Consul-->ZPD: true
ZPD->TiKV: check table exists
alt table exists
TiKV-->ZPD: Yes
ZPD-->Client: response table exists
else table does not exists
TiKV-->ZPD: No
ZPD-->ZPD: validate table, prepare (key, value)
ZPD->TiKV: put (key, value)
TiKV-->ZPD: response
ZPD-->Client: response
end
end
```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.

#### 2.4.7 Drop Table
- SQL:
```sql
DROP TABLE table_0;
```

- TH1 ZPD là node leader

```plantuml
title: Drop Table

Client-->ZPD: drop Table
ZPD-->ZPD: check the database being used by the session
alt database is null
ZPD-->Client: do not connect database
else database is not null
ZPD->Consul: check leader
Consul-->ZPD: true
ZPD->TiKV: get Table
alt table exists
TiKV-->ZPD: Yes
ZPD-->ZPD: gen Key
ZPD->TiKV: Delete table by key
TiKV-->ZPD: response
ZPD-->Client: response
else get table fail
TiKV-->ZPD: No
ZPD-->Client: response fail
end
end
```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.

`Note:` Update drop rows của table (xử lý async) sau khi drop table.

#### 2.4.8 Show Table of Database
- SQL:
```sql
SHOW TABLES;
SHOW TABLES FROM db_0;
```

- TH1 ZPD là node leader

```plantuml
title: Show Tables

Client->ZPD: show Tables
ZPD->Consul: check leader
Consul->ZPD: true
ZPD->TiKV: get databases
alt get databases success
TiKV-->ZPD: sucess
ZPD-->ZPD: check database is exist
alt database is exist
ZPD-->ZPD: yes
ZPD->TiKV: get tables
alt get tables success
TiKV-->ZPD: success
ZPD-->ZPD: prepare result
ZPD-->Client: response
else get tables fail
TiKV-->ZPD: fail
ZPD-->Client: response fail
end 
else database is not exist
ZPD-->ZPD: no
ZPD-->Client: response fail
end
else get database fail
TiKV-->ZPD: fail
ZPD-->Client: fail
end


```

- TH2 ZPD không phải node leader: tương tự trường hợp của create database.


#### 2.4.9 Insert Row
- SQL:
```sql
INSERT INTO table_0(ID, Name, gmail) VALUES (10000000, 'taiptht', 'taiptht@gmail.com'), (20000000, 'thinhda', 'thinhda@gmail.com');
```

```plantuml
title: Insert row

Client->ZPD: insert row
ZPD-->ZPD: check user use database
alt true
ZPD-->ZPD: check table is exists
alt true
ZPD-->ZPD: prepare row data (check type column, not null)
ZPD->TiKV: prepare index (check unique)
alt exist
TiKV-->ZPD: value is exists
ZPD-->Client: Duplicate value
else not exist
TiKV -->ZPD: value is not exists
ZPD-->ZPD: check type insert: single row or multiple rows
alt single
ZPD->TiKV: put (key, value)
TiKV-->ZPD: response
ZPD-->Client: response
else multiple
ZPD->TiKV: BatchPut(keys, values)
TiKV-->ZPD: response
ZPD-->Client: response
end
end
else false
ZPD-->Client: table dose not exitst
end
else false
ZPD--> Client: Do not use database
end
```

#### 2.4.10 Select Row
- SQL: 

```sql
SELECT * FROM table_0;
SELECT ID, Gmail FROM table_0
SELECT * FROM table_0 WHERE ID = 1; (có index)
SELECT * FROM table_0 WHERE gmail = 'taiptht@gmail.com'; (không có index)
```

- Trường hợp không có index
```plantuml
title: Select row

Client->ZPD: insert row
ZPD-->ZPD: check user use database
alt true
ZPD-->ZPD: check table is exists
alt true
ZPD->TiKV: scan data
alt success
TiKV-->ZPD: data
ZPD-->ZPD: prepare data
ZPD-->Client: response
else fail
TiKV-->ZPD: fail
ZPD-->Client: response fail
end
else false
ZPD-->Client: table dose not exitst
end
else false
ZPD--> Client: Do not use database
end
```

- Trường hợp có index
```plantuml
title: Select row

Client->ZPD: insert row
ZPD-->ZPD: check user use database
alt true
ZPD-->ZPD: check table is exists
alt true
ZPD->ZPD: prepare key index
ZPD-->TiKV: get value index row
alt exist
TiKV-->ZPD: value
ZPD-->ZPD: prepare key row
ZPD->TiKV: get value of row
TiKV-->ZPD: data row
ZPD-->ZPD: prepare data
ZPD-->Client: response data
else no exist
TiKV-->ZPD: do not find
ZPD-->Client: response fail
end
else false
ZPD-->Client: table dose not exitst
end
else false
ZPD--> Client: Do not use database
end
```


#### 2.4.11 Delete Row
- SQL:
```sql
DELETE FROM table_0  WHERE ID = 0
DELETE FROM table_0 WHERE gmai ='taiptht@gmail.com'
```

- Trường hợp không có index
```plantuml
title: Delete row

Client->ZPD: delete row
ZPD-->ZPD: check user use database
alt true
ZPD-->ZPD: check table is exists
alt true
ZPD->TiKV: scan data
alt success
TiKV-->ZPD: data
ZPD-->ZPD: find key
alt exist
ZPD->TiKV: delete key-value
TiKV-->ZPD: response
ZPD-->Client: response
else not exist
ZPD-->Client: do not row to delete
end
ZPD-->Client: response
else fail
TiKV-->ZPD: fail
ZPD-->Client: response fail
end
else false
ZPD-->Client: table dose not exitst
end
else false
ZPD--> Client: Do not use database
end
```

- Trường hợp có index

```plantuml
title: Delete row

Client->ZPD: delete row
ZPD-->ZPD: check user use database
alt true
ZPD-->ZPD: check table is exists
alt true
ZPD->ZPD: prepare key index
ZPD-->TiKV: get value index row
alt exist
TiKV-->ZPD: value
ZPD-->ZPD: prepare key row
ZPD->TiKV: delete key-value
TiKV-->ZPD: response
ZPD-->Client: response
else not exists
ZPD-->Client: do not row to delete
ZPD-->Client: response
else no exist
TiKV-->ZPD: do not find
ZPD-->Client: response fail
end
else false
ZPD-->Client: table dose not exitst
end
else false
ZPD--> Client: Do not use database
end
```