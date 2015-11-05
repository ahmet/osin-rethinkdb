package rethinkdb

import (
	"github.com/RangelReale/osin"
	"github.com/mitchellh/mapstructure"
	r "gopkg.in/dancannon/gorethink.v1"
)

const (
	clientsTable = "oauth_clients"
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

// CreateClient inserts a new client to storage
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
