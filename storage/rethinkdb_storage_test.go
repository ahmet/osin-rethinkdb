package RethinkDBStorage

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/RangelReale/osin"
	"github.com/stretchr/testify/require"

	r "gopkg.in/dancannon/gorethink.v1"
)

var (
	Rethink       *r.Session
	RethinkDBName string
)

func init() {
	RethinkDBName = getEnvOrDefault("RETHINKDB_DB", "osin_rethinkdb_storage")

	session, err := r.Connect(getConfig())
	if err != nil {
		panic(err)
	}
	Rethink = session

	dropTestDatabase()
	createTestDatabase()
}

func initTestStorage() *RethinkDBStorage {
	return New(Rethink, RethinkDBName)
}

func createTable(name string) {
	r.DB(RethinkDBName).TableCreate(name).Exec(Rethink)
}

func dropTable(name string) {
	r.DB(RethinkDBName).TableDrop(name).Exec(Rethink)
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

func newClient() *osin.DefaultClient {
	return &osin.DefaultClient{Id: "client", Secret: "secret", RedirectUri: "http://localhost/", UserData: make(map[string]interface{})}
}

func TestCreateClient(t *testing.T) {
	createTable(clientsTable)
	defer dropTable(clientsTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))
}

func TestGetClient(t *testing.T) {
	createTable(clientsTable)
	defer dropTable(clientsTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	clientFound, err := storage.GetClient(client.GetId())
	require.Nil(t, err)
	require.Equal(t, clientFound, client)
}

func TestUpdateClient(t *testing.T) {
	createTable(clientsTable)
	defer dropTable(clientsTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	client.Secret = "secret_changed"
	client.RedirectUri = "http://localhost/changed"

	err := storage.UpdateClient(client)
	require.Nil(t, err)

	clientFound, err := storage.GetClient(client.GetId())
	require.Nil(t, err)
	require.Equal(t, clientFound, client)
}

func TestDeleteClient(t *testing.T) {
	createTable(clientsTable)
	defer dropTable(clientsTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	err := storage.DeleteClient(client)
	require.Nil(t, err)
}

func TestSaveAuthorize(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	data := &osin.AuthorizeData{
		Client:      client,
		Code:        "9999",
		ExpiresIn:   3600,
		CreatedAt:   time.Now(),
		RedirectUri: "http://localhost/",
	}
	require.Nil(t, storage.SaveAuthorize(data))
}

func TestLoadAuthorizeNonExistent(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)

	storage := initTestStorage()
	loadData, err := storage.LoadAuthorize("nonExistentCode")
	require.Nil(t, loadData)
	require.NotNil(t, err)
}

func TestLoadAuthorize(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	data := &osin.AuthorizeData{
		Client:      client,
		Code:        "8888",
		ExpiresIn:   3600,
		CreatedAt:   time.Now(),
		RedirectUri: "http://localhost/",
	}
	require.Nil(t, storage.SaveAuthorize(data))

	loadData, err := storage.LoadAuthorize(data.Code)
	require.Nil(t, err)
	require.False(t, reflect.DeepEqual(loadData, data))
}

func TestRemoveAuthorizeNonExistent(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)

	storage := initTestStorage()
	err := storage.RemoveAuthorize("nonExistentCode")
	require.NotNil(t, err)
}

func TestRemoveAuthorize(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	data := &osin.AuthorizeData{
		Client:      client,
		Code:        "8888",
		ExpiresIn:   3600,
		CreatedAt:   time.Now(),
		RedirectUri: "http://localhost/",
	}
	require.Nil(t, storage.SaveAuthorize(data))

	err := storage.RemoveAuthorize("8888")
	require.Nil(t, err)

	loadData, err := storage.LoadAuthorize(data.Code)
	require.Nil(t, loadData)
	require.NotNil(t, err)
}

func TestSaveAccess(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	createTable(accessTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)
	defer dropTable(accessTable)

	storage := initTestStorage()
	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	authorizeData := &osin.AuthorizeData{
		Client:      client,
		Code:        "8888",
		ExpiresIn:   3600,
		CreatedAt:   time.Now(),
		RedirectUri: "http://localhost/",
	}
	require.Nil(t, storage.SaveAuthorize(authorizeData))

	accessData := &osin.AccessData{
		Client:        authorizeData.Client,
		AuthorizeData: authorizeData,
		AccessToken:   "8888",
		RefreshToken:  "r8888",
		ExpiresIn:     3600,
		CreatedAt:     time.Now(),
	}

	require.Nil(t, storage.SaveAccess(accessData))
}
