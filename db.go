package vech

import (
	"errors"
	"strings"
)

var (
	ErrDbIsInitiated = errors.New("database is already initiated")
	ErrVectorSize    = errors.New("vector size must be greater than zero")
)

type config struct {
	VectorSize int
}

// Db structrue is database instance
type Db struct {
	path        string
	config      *config
	storageType StorageType
}

// CreateDbOptions are used for database creation
type CreateDbOptions struct {
	VectorSize  int
	StorageType StorageType
	Path        string
}

// CreateDb creates new database
func CreateDb(opt *CreateDbOptions) (*Db, error) {
	if opt.VectorSize < 0 {
		return nil, ErrVectorSize
	}
	config := config{VectorSize: opt.VectorSize}
	path := strings.TrimSuffix(opt.Path, "/")
	db := Db{path: path, config: &config, storageType: opt.StorageType}
	switch opt.StorageType {
	case FileSystem:
		if err := checkOrCreateDir(path); err != nil {
			return nil, err
		}
		if err := saveConfig(path+"/vech.cfg", &config); err != nil {
			return nil, err
		}
	case Memory:
	}
	return &db, nil
}

// OpenFileDb open file database
func OpenFileDb(path string) (*Db, error) {
	path = strings.TrimSuffix(path, "/")
	config, err := readConfig(path + "/vech.cfg")
	if err != nil {
		return nil, err
	}
	return &Db{path: path, config: config, storageType: FileSystem}, nil
}

// OpenCollection opens collection if it exists, else it creates new collection
func (db *Db) OpenCollection(name string) (*Collection, error) {
	var id, dt storage
	var err error
	switch db.storageType {
	case FileSystem:
		id, err = openFileStorage(db.path + "/" + name + ".idx")
		if err != nil {
			return nil, err
		}
		dt, err = openFileStorage(db.path + "/" + name + ".data")
		if err != nil {
			return nil, err
		}
	case Memory:
		id = newMemoryStorage()
		dt = newMemoryStorage()
	}
	idxSize := id.size()
	c := Collection{
		indexStorage: id,
		dataStorage:  dt,
		vectorSize:   db.config.VectorSize,
		recordSize:   db.config.VectorSize*4 + 16,
		dataSize:     dt.size(),
		index:        make([]byte, idxSize),
	}
	if idxSize > 0 {
		reader, err := id.reader(0)
		if err != nil {
			return nil, err
		}
		defer id.closeReader()
		nread, err := reader.Read(c.index)
		if err != nil {
			return nil, err
		}
		if nread != idxSize {
			return nil, ErrCorruptedDb
		}
	}
	return &c, nil
}
