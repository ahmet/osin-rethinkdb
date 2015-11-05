package rethinkdb

import (
	"os"
	"testing"

	"github.com/RangelReale/osin"
	"github.com/stretchr/testify/require"

	r "gopkg.in/dancannon/gorethink.v1"
)

var Rethink *r.Session
var RethinkDBName string

func init() {
	RethinkDBName = getEnvOrDefault("RETHINKDB_DB", "osin_rethinkdb_storage")

	session, err := r.Connect(getConfig())
	if err != nil {
		panic(err)
	}
	Rethink = session

	dropTestDatabase()
}

func initTestStorage() *RethinkStorage {
	return New(Rethink, RethinkDBName)
}

func createTable(name string) {
	r.DB(RethinkDBName).TableCreate(name).Exec(Rethink)
}

func createTestDatabase() {
	r.DBCreate(RethinkDBName).RunWrite(Rethink)
}

func dropTestDatabase() {
	r.DBDrop(RethinkDBName).RunWrite(Rethink)
}

func getEnvOrDefault(key, def string) string {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	return value
}

func getConfig() r.ConnectOpts {
	address := getEnvOrDefault("RETHINKDB_URL", "localhost:28015")

	return r.ConnectOpts{
		Address:  address,
		Database: RethinkDBName}
}

func TestClientCreate(t *testing.T) {
	createTestDatabase()
	createTable("oauth_clients")
	defer dropTestDatabase()

	storage := initTestStorage()
	client := &osin.DefaultClient{Id: "first_client", Secret: "secret1", RedirectUri: "http://localhost/first", UserData: make(map[string]interface{})}
	require.Nil(t, storage.CreateClient(client))
}

func TestClientGet(t *testing.T) {
	createTestDatabase()
	createTable("oauth_clients")
	defer dropTestDatabase()

	storage := initTestStorage()
	client := &osin.DefaultClient{Id: "second_client", Secret: "secret2", RedirectUri: "http://localhost/second", UserData: make(map[string]interface{})}
	require.Nil(t, storage.CreateClient(client))

	clientFound, err := storage.GetClient(client.GetId())
	require.Nil(t, err)
	require.Equal(t, clientFound, client)
}

func TestClientUpdate(t *testing.T) {
	createTestDatabase()
	createTable("oauth_clients")
	defer dropTestDatabase()

	storage := initTestStorage()
	client := &osin.DefaultClient{Id: "third_client", Secret: "secret3", RedirectUri: "http://localhost/third", UserData: make(map[string]interface{})}
	require.Nil(t, storage.CreateClient(client))

	client.Secret = "secret3_changed"
	client.RedirectUri = "http://localhost/third_changed"

	err := storage.UpdateClient(client)
	require.Nil(t, err)

	clientFound, err := storage.GetClient(client.GetId())
	require.Nil(t, err)
	require.Equal(t, clientFound, client)
}

func TestClientDelete(t *testing.T) {
	createTestDatabase()
	createTable("oauth_clients")
	defer dropTestDatabase()

	storage := initTestStorage()
	client := &osin.DefaultClient{Id: "first_client", Secret: "secret1", RedirectUri: "http://localhost/first", UserData: make(map[string]interface{})}
	require.Nil(t, storage.CreateClient(client))

	err := storage.DeleteClient(client)
	require.Nil(t, err)
}
