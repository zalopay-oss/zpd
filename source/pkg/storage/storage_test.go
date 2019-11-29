package storage

import (
	"context"
	"testing"
)

func TestConnectDatabase(t *testing.T) {
	_, err := NewRawTiKV(context.Background(), []string{"pd1:2379", "pd2:2379", "pd3:2379"})
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestConnectDisconnectDB(t *testing.T) {
	storage, err := NewRawTiKV(context.Background(), []string{"pd1:2379", "pd2:2379", "pd3:2379"})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = storage.CloseDB()
	if err != nil {
		t.Fatalf(err.Error())
	}
}
