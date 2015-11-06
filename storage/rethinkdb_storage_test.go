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

func newAuthorizeData(client *osin.DefaultClient) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:      client,
		Code:        "8888",
		ExpiresIn:   3600,
		CreatedAt:   time.Now(),
		RedirectUri: "http://localhost/",
	}
}

func newAccessData(authorizeData *osin.AuthorizeData) *osin.AccessData {
	return &osin.AccessData{
		Client:        authorizeData.Client,
		AuthorizeData: authorizeData,
		AccessToken:   "8888",
		RefreshToken:  "r8888",
		ExpiresIn:     3600,
		CreatedAt:     time.Now(),
	}
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

	authorizeData := newAuthorizeData(client)
	require.Nil(t, storage.SaveAuthorize(authorizeData))
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

	authorizeData := newAuthorizeData(client)
	require.Nil(t, storage.SaveAuthorize(authorizeData))

	loadData, err := storage.LoadAuthorize(authorizeData.Code)
	require.Nil(t, err)
	require.False(t, reflect.DeepEqual(loadData, authorizeData))
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

	authorizeData := newAuthorizeData(client)
	require.Nil(t, storage.SaveAuthorize(authorizeData))

	err := storage.RemoveAuthorize(authorizeData.Code)
	require.Nil(t, err)

	loadData, err := storage.LoadAuthorize(authorizeData.Code)
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

	authorizeData := newAuthorizeData(client)
	require.Nil(t, storage.SaveAuthorize(authorizeData))

	accessData := newAccessData(authorizeData)
	require.Nil(t, storage.SaveAccess(accessData))
}

func TestLoadAccessNonExistent(t *testing.T) {
	createTable(accessTable)
	defer dropTable(accessTable)

	storage := initTestStorage()

	loadData, err := storage.LoadAccess("nonExistentToken")
	require.Nil(t, loadData)
	require.NotNil(t, err)
}

func TestLoadAccess(t *testing.T) {
	createTable(clientsTable)
	createTable(authorizeTable)
	createTable(accessTable)
	defer dropTable(clientsTable)
	defer dropTable(authorizeTable)
	defer dropTable(accessTable)

	storage := initTestStorage()

	client := newClient()
	require.Nil(t, storage.CreateClient(client))

	authorizeData := newAuthorizeData(client)
	require.Nil(t, storage.SaveAuthorize(authorizeData))

	accessData := newAccessData(authorizeData)
	require.Nil(t, storage.SaveAccess(accessData))

	loadData, err := storage.LoadAccess(accessData.AccessToken)
	require.Nil(t, err)
	require.False(t, reflect.DeepEqual(loadData, accessData))
}
