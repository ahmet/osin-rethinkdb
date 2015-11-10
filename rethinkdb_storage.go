package RethinkDBStorage

import (
	"github.com/RangelReale/osin"
	"github.com/mitchellh/mapstructure"
	r "gopkg.in/dancannon/gorethink.v1"
)

const (
	clientsTable      = "oauth_clients"
	authorizeTable    = "oauth_authorize_data"
	accessTable       = "oauth_access_data"
	accessTokenField  = "AccessToken"
	refreshTokenField = "RefreshToken"
)

// RethinkDBStorage implements storage for osin
type RethinkDBStorage struct {
	session *r.Session
}

// New initializes and returns a new RethinkDBStorage
func New(session *r.Session) *RethinkDBStorage {
	storage := &RethinkDBStorage{session}
	return storage
}

// Clone the storage if needed.
func (s *RethinkDBStorage) Clone() osin.Storage {
	return s
}

// Close the resources the Storage potentially holds
func (s *RethinkDBStorage) Close() {}

// CreateClient inserts a new client
func (s *RethinkDBStorage) CreateClient(c osin.Client) error {
	_, err := r.Table(clientsTable).Insert(c).RunWrite(s.session)
	return err
}

// GetClient returns client with given ID
func (s *RethinkDBStorage) GetClient(clientID string) (osin.Client, error) {
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
func (s *RethinkDBStorage) UpdateClient(c osin.Client) error {
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
func (s *RethinkDBStorage) DeleteClient(c osin.Client) error {
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
func (s *RethinkDBStorage) SaveAuthorize(data *osin.AuthorizeData) error {
	_, err := r.Table(authorizeTable).Insert(data).RunWrite(s.session)
	return err
}

// LoadAuthorize gets authorization data with given code
func (s *RethinkDBStorage) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	result, err := r.Table(authorizeTable).Filter(r.Row.Field("Code").Eq(code)).Run(s.session)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return nil, err
	}

	var client osin.Client
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
func (s *RethinkDBStorage) RemoveAuthorize(code string) error {
	result, err := r.Table(authorizeTable).Filter(r.Row.Field("Code").Eq(code)).Run(s.session)
	if err != nil {
		return err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return err
	}

	_, err = r.Table(authorizeTable).Get(dataMap["id"]).Delete().RunWrite(s.session)
	return err
}

// SaveAccess creates a new access data
func (s *RethinkDBStorage) SaveAccess(data *osin.AccessData) error {
	_, err := r.Table(accessTable).Insert(data).RunWrite(s.session)
	return err
}

// LoadAccess gets access data with given access token
func (s *RethinkDBStorage) LoadAccess(accessToken string) (*osin.AccessData, error) {
	return s.getAccessData(accessTokenField, accessToken)
}

// RemoveAccess deletes AccessData with given access token
func (s *RethinkDBStorage) RemoveAccess(accessToken string) error {
	return s.removeAccessData(accessTokenField, accessToken)
}

// LoadRefresh gets access data with given refresh token
func (s *RethinkDBStorage) LoadRefresh(refreshToken string) (*osin.AccessData, error) {
	return s.getAccessData(refreshTokenField, refreshToken)
}

// RemoveRefresh deletes AccessData with given refresh token
func (s *RethinkDBStorage) RemoveRefresh(refreshToken string) error {
	return s.removeAccessData(refreshTokenField, refreshToken)
}

// getAccessData is a common function to get AccessData by field
func (s *RethinkDBStorage) getAccessData(fieldName, token string) (*osin.AccessData, error) {
	result, err := r.Table(accessTable).Filter(r.Row.Field(fieldName).Eq(token)).Run(s.session)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return nil, err
	}

	var client osin.Client
	clientID := dataMap["Client"].(map[string]interface{})["Id"].(string)
	client, err = s.GetClient(clientID)
	if err != nil {
		return nil, err
	}
	dataMap["Client"] = client

	if authorizeData := dataMap["AuthorizeData"]; authorizeData != nil {
		if authorizeDataClient := authorizeData.(map[string]interface{})["Client"]; authorizeDataClient != nil {
			var authorizeDataClientStruct osin.Client
			if authorizeDataClientID := authorizeDataClient.(map[string]interface{})["Id"]; authorizeDataClientID != nil {
				authorizeDataClientStruct, err = s.GetClient(authorizeDataClientID.(string))
				if err != nil {
					return nil, err
				}
				dataMap["AuthorizeData"].(map[string]interface{})["Client"] = authorizeDataClientStruct
			}
		}
	}

	var dataStruct osin.AccessData
	err = mapstructure.Decode(dataMap, &dataStruct)
	if err != nil {
		return nil, err
	}

	return &dataStruct, nil
}

// removeAccessData is a common function to remove AccessData by field
func (s *RethinkDBStorage) removeAccessData(fieldName, token string) error {
	result, err := r.Table(accessTable).Filter(r.Row.Field(fieldName).Eq(token)).Run(s.session)
	if err != nil {
		return err
	}
	defer result.Close()

	var dataMap map[string]interface{}
	err = result.One(&dataMap)
	if err != nil {
		return err
	}

	_, err = r.Table(accessTable).Get(dataMap["id"]).Delete().RunWrite(s.session)
	return err
}
