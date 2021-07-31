package client

import (
	"strconv"
	"sync"
	"testing"

	"zpd/configs"

	zpd_proto "zpd/pkg/public-api"
	"zpd/pkg/util"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func loadConfig() *configs.ZPDServiceConfig {
	config := &configs.ZPDServiceConfig{}
	configs.LoadConfig()
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal("load config: ", err)
	}

	return config
}
func TestClientPing(t *testing.T) {
	cfg := loadConfig()
	client := NewClient(cfg)

	pong, err := client.Ping()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	log.Info(pong)
	client.Close()
}

func runClientConnect(t *testing.T, wg *sync.WaitGroup, cfg *configs.ZPDServiceConfig, dbName string) {
	defer wg.Done()
	client := NewClient(cfg)
	defer client.Close()

	msg, err := client.ConnectDatabase(dbName)
	if err != nil {
		t.Log(err.Error())
		client.Close()
	}
	log.Info(msg)

	msg, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Log(err.Error())
	}
	log.Info(msg)
}

// TestClientConnectSuccess1 connect success but does not exsits dbname
func TestClientConnectSuccess1(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10

	cfg := loadConfig()

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientConnect(t, &wg, cfg, "db_"+strconv.Itoa(i))
	}

	wg.Wait()
	log.Info("Test Client Connect Done")
}

func TestClientCloseConnectionDB(t *testing.T) {
	cfg := loadConfig()

	client := NewClient(cfg)
	msg, err := client.ConnectDatabase("db")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}
	log.Info(msg)

	client2 := NewClient(cfg)
	msg, err = client2.ConnectDatabase("db")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}
	log.Info(msg)

	msg, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}
	log.Info(msg)

	msg, err = client2.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}
	log.Info(msg)

	client.Close()
	client2.Close()
}

func runClientCreateDB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD, dbName string) {
	defer wg.Done()

	sql := "Create database " + dbName + ";"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)
}

func TestClientExecuteCreateDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 5
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientCreateDB(t, &wg, client, "db_"+strconv.Itoa(i))
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientsCreateDB(t *testing.T, wg *sync.WaitGroup, cfg *configs.ZPDServiceConfig, dbName string) {
	defer wg.Done()
	client := NewClient(cfg)
	defer client.Close()

	_, err := client.ConnectDatabase("*")
	if err != nil {
		t.Log(err)
		return
	}

	sql := "Create database " + dbName + ";"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Log(err)
	}
}

func TestClientsExecuteCreateDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10

	cfg := loadConfig()

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientsCreateDB(t, &wg, cfg, "tb_"+strconv.Itoa(i))
	}

	wg.Wait()
}

func runClientUseDB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD, dbName string) {
	defer wg.Done()
	sql := "Use " + dbName + ";"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)
}

func TestClientExecuteUseDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientUseDB(t, &wg, client, "db_"+strconv.Itoa(i))
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	client.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func runClientsUseDB(t *testing.T, wg *sync.WaitGroup, cfg *configs.ZPDServiceConfig, dbName string) {
	defer wg.Done()
	client := NewClient(cfg)
	defer client.Close()

	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Log(err)
		return
	}

	sql := "Use " + dbName + ";"

	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Log(err)
	}
}

func TestClientsExecuteUseDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1

	cfg := loadConfig()

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientsUseDB(t, &wg, cfg, "db_"+strconv.Itoa(i))
	}

	wg.Wait()
}

func runClientDropDB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD, dbName string) {
	defer wg.Done()
	sql := "Drop database " + dbName + ";"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)
}

func TestClientExecuteDropDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientDropDB(t, &wg, client, "db_"+strconv.Itoa(i))
	}

	wg.Wait()

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientConnect(t, &wg, cfg, "db_"+strconv.Itoa(i))
	}
	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientShowDB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD) {
	defer wg.Done()
	sql := "Show databases"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_SHOWDATABASE, sql)
	if err != nil {
		t.Log(err)
		return
	}

	if res.Status.Code == 0 {
		t.Errorf(res.Status.Error)
		return
	}

	generate := util.NewGenerate()
	data, err := generate.DecodeDataProto(res.Type, res.Data)
	if err != nil {
		t.Log(err)
		return
	}

	log.Info(data)
}

func TestClientExecuteShowDB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientShowDB(t, &wg, client)
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

// Fail: client do not use database
func TestClientExecuteCreateTBFail1(t *testing.T) {
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	sql := "CREATE TABLE `tb` (`id` int(200) NOT NULL, `name` varchar (10), PRIMARY KEY (`id`));"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	if res.Status.Code == 0 {
		t.Errorf(res.Status.Error)
		return
	}

	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	log.Info(res)
	client.Close()
}

// Fail: duplicate column
func TestClientExecuteCreateTBFail2(t *testing.T) {
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	sql := "CREATE TABLE `tb` (`id` int(200) NOT NULL, `name` varchar (10), `name` varchar (10), PRIMARY KEY (`id`));"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	if res.Status.Code == 0 {
		t.Errorf(res.Status.Error)
		return
	}

	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	log.Info(res)
	client.Close()
}

func runClientCreateTB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD, tbName string) {
	defer wg.Done()
	sql := "CREATE TABLE " + tbName + " (`id` int(200) NOT NULL, `name` varchar (10), `gmail` varchar (20), PRIMARY KEY (`id`));"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}

	if res.Status.Code == 0 {
		t.Errorf(res.Status.Error)
		return
	}

	if err != nil {
		t.Log(err)
		return
	}

	log.Info(res)
}

func TestClientExecuteCreateTBSuccess(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientCreateTB(t, &wg, client, "tb_"+strconv.Itoa(i))
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientShowTable(t *testing.T, wg *sync.WaitGroup, client *ClientZPD) {
	defer wg.Done()
	// sql := "Show tables"
	// sql := "Show tables from test0"
	sql := "Show tables"

	res, err := client.ExecuteStatement(zpd_proto.SQLType_SHOWTABLE, sql)
	if err != nil {
		t.Log(err)
		return
	}

	if res.Status.Code == 0 {
		t.Errorf(res.Status.Error)
		return
	}

	generate := util.NewGenerate()
	data, err := generate.DecodeDataProto(res.Type, res.Data)
	if err != nil {
		t.Log(err)
		return
	}

	log.Info(data.(*zpd_proto.NameTables).Nametables)
}

func TestClientExecuteShowTBSuccess(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientShowTable(t, &wg, client)
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

// do not use database
func TestClientExecuteShowTBFail1(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientShowTable(t, &wg, client)
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

// database is not exists
func TestClientExecuteShowTBFail2(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientShowTable(t, &wg, client)
	}

	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientDropTB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD, tbName string) {
	defer wg.Done()
	sql := "Drop table " + tbName + ";"
	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)
}

func TestClientExecuteDropTB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 10
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientDropTB(t, &wg, client, "tb_"+strconv.Itoa(i))
	}
	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientInserRowTB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD) {
	defer wg.Done()
	sql := "INSERT INTO tb_1(id, name, gmail)" +
		" VALUES (10, 'tai2', 'pthtantai2@gmail.com'), (11, 'loc2', 'loc2@gmail.com'), (12, 'thinh2', 'thinhda2@gmail.com');"

	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}
	log.Info(res)
}

func TestClientExecuteInsertRowTB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientInserRowTB(t, &wg, client)
	}
	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientSelectRowTB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD) {
	defer wg.Done()
	sql := "Select * From tb_1;"
	// sql := "SELECT ID, gmail FROM tb_1 WHERE ID = 11"

	res, err := client.ExecuteStatement(zpd_proto.SQLType_SELECT, sql)
	if err != nil {
		t.Log(err)
		return
	}

	log.Info(res.Status)

	generate := util.NewGenerate()
	data, err := generate.DecodeDataProto(res.Type, res.Data)
	if err != nil {
		t.Log(err)
		return
	}

	log.Info(data.(*zpd_proto.Rows))
}

func TestClientExecuteSelectRowTB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientSelectRowTB(t, &wg, client)
	}
	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}

func runClientDeleteRowTB(t *testing.T, wg *sync.WaitGroup, client *ClientZPD) {
	defer wg.Done()
	sql := "DELETE FROM tb_1 WHERE ID = 10"

	res, err := client.ExecuteStatement(zpd_proto.SQLType_DEFAULT, sql)
	if err != nil {
		t.Log(err)
		return
	}

	log.Info(res.Status)
}

func TestClientExecuteDeleteRowTB(t *testing.T) {
	var wg sync.WaitGroup
	numClient := 1
	cfg := loadConfig()

	client := NewClient(cfg)
	_, err := client.ConnectDatabase("db_1")
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	for i := 0; i < numClient; i++ {
		wg.Add(1)
		go runClientDeleteRowTB(t, &wg, client)
	}
	wg.Wait()

	_, err = client.CloseConnectionDatabase()
	if err != nil {
		t.Fatalf(err.Error())
		client.Close()
	}

	client.Close()
}
