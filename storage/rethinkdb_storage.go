package RethinkDBStorage

import (
	"github.com/RangelReale/osin"
	"github.com/mitchellh/mapstructure"
	r "gopkg.in/dancannon/gorethink.v1"
)

const (
	clientsTable        = "oauth_clients"
	authorizationsTable = "oauth_authorizations"
)

// RethinkStorage implements storage for osin
type RethinkStorage struct {
	dbName  string
	session *r.Session
}

// New initializes and returns a new RethinkStorage
func New(session *r.Session, dbName string) *RethinkStorage {
	storage := &RethinkStorage{dbName, session}
	return storage
}

// CreateClient inserts a new client
func (s *RethinkStorage) CreateClient(c osin.Client) error {
	_, err := r.Table(clientsTable).Insert(c).RunWrite(s.session)
	return err
}

// GetClient returns client with given ID
func (s *RethinkStorage) GetClient(clientID string) (*osin.DefaultClient, error) {
	result, err := r.Table(clientsTable).Filter(r.Row.Field("Id").Eq(clientID)).Run(s.session)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var clientMap map[string]interface{}
	err = result.One(&clientMap)
	if err != nil {
		return nil, err
	}

	var clientStruct osin.DefaultClient
	err = mapstructure.Decode(clientMap, &clientStruct)
	if err != nil {
		return nil, err
	}

	return &clientStruct, nil
}

// UpdateClient updates given client
func (s *RethinkStorage) UpdateClient(c osin.Client) error {
	result, err := r.Table(clientsTable).Filter(r.Row.Field("Id").Eq(c.GetId())).Run(s.session)
	if err != nil {
		return err
	}
	defer result.Close()

	var clientMap map[string]interface{}
	err = result.One(&clientMap)
	if err != nil {
		return err
	}

	_, err = r.Table(clientsTable).Get(clientMap["id"]).Update(c).RunWrite(s.session)
	return err
}

// DeleteClient deletes given client
func (s *RethinkStorage) DeleteClient(c osin.Client) error {
	result, err := r.Table(clientsTable).Filter(r.Row.Field("Id").Eq(c.GetId())).Run(s.session)
	if err != nil {
		return err
	}
	defer result.Close()

	var clientMap map[string]interface{}
	err = result.One(&clientMap)
	if err != nil {
		return err
	}

	_, err = r.Table(clientsTable).Get(clientMap["id"]).Delete().RunWrite(s.session)
	return err
}

// SaveAuthorize creates a new authorization
func (s *RethinkStorage) SaveAuthorize(data *osin.AuthorizeData) error {
	_, err := r.Table(authorizationsTable).Insert(data).RunWrite(s.session)
	return err
}

// LoadAuthorize gets authorization data with given code
func (s *RethinkStorage) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	result, err := r.Table(authorizationsTable).Filter(r.Row.Field("Code").Eq(code)).Run(s.session)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return nil, err
	}

	var client *osin.DefaultClient
	clientID := dataMap["Client"].(map[string]interface{})["Id"].(string)
	client, err = s.GetClient(clientID)
	if err != nil {
		return nil, err
	}
	dataMap["Client"] = client

	var dataStruct osin.AuthorizeData
	err = mapstructure.Decode(dataMap, &dataStruct)
	if err != nil {
		return nil, err
	}

	return &dataStruct, nil
}

// RemoveAuthorize deletes given authorization
func (s *RethinkStorage) RemoveAuthorize(code string) error {
	result, err := r.Table(authorizationsTable).Filter(r.Row.Field("Code").Eq(code)).Run(s.session)
	if err != nil {
		return err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return err
	}

	_, err = r.Table(authorizationsTable).Get(dataMap["id"]).Delete().RunWrite(s.session)
	return err
}
