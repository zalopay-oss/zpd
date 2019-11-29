package benchmark

import (
	"strconv"
	"sync/atomic"
	zpd_proto "zpd/pkg/public-api"

	"github.com/myzhan/boomer"
	log "github.com/sirupsen/logrus"
)

const (
	CONNECT_DATABASE = "connect_database"
	CREATE_DATABASE  = "create_database"
	SHOW_DATABASE    = "show_database"
	DROP_DATABASE    = "drop_database"
	CREATE_TABLE     = "create_table"
	SHOW_TABLE       = "show_table"
	DROP_TABLE       = "drop_table"
	INSERT_ROW       = "insert_row"
	SELECT_ROW_STAR  = "select_row_star"
	SELECT_ROW       = "select_row"
	SELECT_ROW_INDEX = "select_row_index"
	DELETE_ROW_INDEX = "delete_row_index"
	DELETE_ROW       = "delete_row"
)

var count int64
var factoryManagerClient *FactoryManagerClient

//BoomerClient boomer client
type BoomerClient struct {
}

//LoadFactoryManagerClient load manager client
func (boomerClient *BoomerClient) LoadFactoryManagerClient(factory *FactoryManagerClient) {
	factoryManagerClient = factory
	log.Info("Prepare connection Database: ", factory.config.DBName)
	factoryManagerClient.prepareConnectionToZPD(factory.config.DBName)
}

//ConnectDatabase ping
func (boomerClient *BoomerClient) ConnectDatabase() {
	client := factoryManagerClient.NewClient()
	start := boomer.Now()
	pong, err := client.ConnectDatabase("test0")
	elapsed := boomer.Now() - start

	client.CloseConnectionDatabase()
	client.Close()

	if err != nil {
		log.Error("Connect database error: ", err)
		boomer.RecordFailure("tcp", "Connect database error", elapsed, err.Error())
	} else {
		boomer.RecordSuccess("tcp", "Connect database", elapsed, int64(pong.XXX_Size()))
	}
}

// CreateDatabase create database
func (boomerClient *BoomerClient) CreateDatabase() {
	client := factoryManagerClient.NewClient()
	sql := "CREATE DATABASE db_" + strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Create database error: ", err)
		boomer.RecordFailure("tcp", "Create database error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Create database error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Create database error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Create database", elapsed, int64(res.XXX_Size()))
	}
}

// ShowDatabase show database
func (boomerClient *BoomerClient) ShowDatabase() {
	client := factoryManagerClient.NewClient()
	sql := "SHOW databases"
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SHOWDATABASE, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Show database error: ", err)
		boomer.RecordFailure("tcp", "Show database error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Show database error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Show database error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Show database", elapsed, int64(res.XXX_Size()))
	}
}

// DropDatabase drop database
func (boomerClient *BoomerClient) DropDatabase() {
	client := factoryManagerClient.NewClient()
	sql := "DROP database db_" + strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Drop database error: ", err)
		boomer.RecordFailure("tcp", "Drop database error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Drop database error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Drop database error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Drop database", elapsed, int64(res.XXX_Size()))
	}
}

// CreateTable create table
func (boomerClient *BoomerClient) CreateTable() {
	tbName := "tb_" + strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "CREATE TABLE " + tbName + " (`id` int(200) NOT NULL, `name` varchar (10), `gmail` varchar (20), PRIMARY KEY (`id`));"
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Create table error: ", err)
		boomer.RecordFailure("tcp", "Create table error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Create table error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Create table error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Create table", elapsed, int64(res.XXX_Size()))
	}
}

// ShowTable show table
func (boomerClient *BoomerClient) ShowTable() {
	client := factoryManagerClient.NewClient()
	sql := "SHOW tables"
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SHOWTABLE, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Show table error: ", err)
		boomer.RecordFailure("tcp", "Show table error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Show table error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Show table error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Show table", elapsed, int64(res.XXX_Size()))
	}
}

// DropTable drop table
func (boomerClient *BoomerClient) DropTable() {
	client := factoryManagerClient.NewClient()
	sql := "DROP table tb_" + strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Drop table error: ", err)
		boomer.RecordFailure("tcp", "Drop table error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Drop table error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Drop table error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Drop table", elapsed, int64(res.XXX_Size()))
	}
}

// InserRow drop table
func (boomerClient *BoomerClient) InserRow() {
	ID := strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "INSERT INTO tb_1(ID, name, gmail)" +
		" VALUES (" + ID + ", 'taiptht_" + ID + "'," + " 'pthtantai_" + ID + "@gmail.com')"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Insert Row error: ", err)
		boomer.RecordFailure("tcp", "Insert Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Insert Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Insert Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Insert Row", elapsed, int64(res.XXX_Size()))
	}
}

// SelectRowStar select row
func (boomerClient *BoomerClient) SelectRowStar() {
	client := factoryManagerClient.NewClient()
	sql := "SELECT * FROM tb_1;"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SELECT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Select Row error: ", err)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Select Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Select Row", elapsed, int64(res.XXX_Size()))
	}
}

// SelectRowHaveIndex select row have index
func (boomerClient *BoomerClient) SelectRowHaveIndex() {
	// ID := strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "SELECT * FROM tb_1 WHERE id = 1;"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SELECT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Select Row error: ", err)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Select Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Select Row", elapsed, int64(res.XXX_Size()))
	}
}

// SelectRow select row
func (boomerClient *BoomerClient) SelectRow() {
	// ID := strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "SELECT * FROM tb_1 WHERE name = 'taiptht_1000';"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SELECT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Select Row error: ", err)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Select Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Select Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Select Row", elapsed, int64(res.XXX_Size()))
	}
}

// DeleteRow delete row
func (boomerClient *BoomerClient) DeleteRow() {
	ID := strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "DELETE FROM tb_1 WHERE name = 'taiptht_" + ID + "';"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Delete Row error: ", err)
		boomer.RecordFailure("tcp", "Delete Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Delete Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Delete Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Delete Row", elapsed, int64(res.XXX_Size()))
	}
}

// DeleteRowHaveIndex delete row have index
func (boomerClient *BoomerClient) DeleteRowHaveIndex() {
	ID := strconv.FormatInt(int64(atomic.AddInt64(&count, 1)), 10)
	client := factoryManagerClient.NewClient()
	sql := "DELETE FROM tb_1 WHERE ID = " + ID + ";"

	start := boomer.Now()
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	elapsed := boomer.Now() - start

	client.Close()

	if err != nil {
		log.Error("Delete Row error: ", err)
		boomer.RecordFailure("tcp", "Delete Row error", elapsed, err.Error())
	} else if res.Status.Error != "" {
		log.Error("Delete Row error: ", res.Status.Error)
		boomer.RecordFailure("tcp", "Delete Row error", elapsed, res.Status.Error)
	} else {
		boomer.RecordSuccess("tcp", "Delete Row", elapsed, int64(res.XXX_Size()))
	}
}

//LoadTask load task
func (boomerClient *BoomerClient) LoadTask(nameTask string, weight int) ([]*boomer.Task, error) {
	taskList := make([]*boomer.Task, 0)

	taskPing := boomerClient.createTask(nameTask, weight)
	taskList = append(taskList, taskPing)

	return taskList, nil
}

//getFuncTask get function task
func (boomerClient *BoomerClient) getFuncTask(nameTask string) func() {
	switch nameTask {
	case CONNECT_DATABASE:
		return boomerClient.ConnectDatabase
	case CREATE_DATABASE:
		return boomerClient.CreateDatabase
	case SHOW_DATABASE:
		return boomerClient.ShowDatabase
	case DROP_DATABASE:
		return boomerClient.DropDatabase
	case CREATE_TABLE:
		return boomerClient.CreateTable
	case SHOW_TABLE:
		return boomerClient.ShowTable
	case DROP_TABLE:
		return boomerClient.DropTable
	case INSERT_ROW:
		return boomerClient.InserRow
	case SELECT_ROW:
		return boomerClient.SelectRow
	case SELECT_ROW_STAR:
		return boomerClient.SelectRowStar
	case SELECT_ROW_INDEX:
		return boomerClient.SelectRowHaveIndex
	case DELETE_ROW_INDEX:
		return boomerClient.DeleteRowHaveIndex
	case DELETE_ROW:
		return boomerClient.DeleteRow
	default:
		return nil
	}

	return nil
}

//createTask create task
func (boomerClient *BoomerClient) createTask(nameTask string, weight int) *boomer.Task {
	fun := boomerClient.getFuncTask(nameTask)
	if fun == nil {
		log.Fatal("Name Task do not valid")
	}

	return &boomer.Task{
		Name:   nameTask,
		Weight: weight,
		Fn:     fun,
	}
}

//RunTask run task
func (boomerClient *BoomerClient) RunTask(tasks []*boomer.Task) {
	boomer.Events.Subscribe("boomer:quit", func() {
		log.Info("Close all connection")
		factoryManagerClient.CloseAllConnection()
	})

	boomer.Run(tasks...)
}
