package vech

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrFileAppend   = errors.New("error opening file for append")
	ErrFileRead     = errors.New("error opening file for reading")
	ErrSeek         = errors.New("seek error")
	ErrPathIsFile   = errors.New("path is expected to be directory, but points to the file")
	ErrPathIsDir    = errors.New("path is expected to be file, but points to the directory")
	ErrConfigAbsent = errors.New("config does not exist")
	ErrReadConfig   = errors.New("error reading database config")
	ErrWriteConfig  = errors.New("error writing database config")
	ErrCreateDir    = errors.New("error creating database dir")
	ErrCreateFile   = errors.New("error creating file")
)

type StorageType int

const (
	FileSystem StorageType = iota
	Memory
)

type storage interface {
	size() int
	writer() (io.Writer, error)
	closeWriter() error
	reader(position int) (io.Reader, error)
	closeReader() error
}

type fileStorage struct {
	path string
	rdf  *os.File
	wrf  *os.File
}

func openFileStorage(path string) (*fileStorage, error) {
	fs := fileStorage{path: path}
	stat, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	} else {
		if stat.IsDir() {
			return nil, fmt.Errorf("%w: %s", ErrPathIsDir, path)
		}
		return &fs, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrCreateFile, err.Error(), path)
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}
	return &fs, nil
}

func (fs *fileStorage) size() int {
	s, err := os.Stat(fs.path)
	if err != nil {
		return 0
	}
	return int(s.Size())
}

func (fs *fileStorage) writer() (io.Writer, error) {
	if fs.wrf != nil {
		return fs.wrf, nil
	}
	var err error
	fs.wrf, err = os.OpenFile(fs.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileAppend, err.Error(), fs.path)
	}
	return fs.wrf, nil
}

func (fs *fileStorage) closeWriter() error {
	if fs.wrf == nil {
		return nil
	}
	err := fs.wrf.Close()
	fs.wrf = nil
	return err
}

func (fs *fileStorage) reader(position int) (io.Reader, error) {
	if position < 0 {
		panic("negative file position")
	}
	if position >= fs.size() {
		return nil, fmt.Errorf("%w: position is greater than storage size", ErrSeek)
	}

	if fs.rdf == nil {
		var err error
		fs.rdf, err = os.Open(fs.path)
		if err != nil {
			return nil, fmt.Errorf("%w: %s %s", ErrFileRead, err.Error(), fs.path)
		}
	}
	_, err := fs.rdf.Seek(int64(position), 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrSeek, err.Error(), fs.path)
	}
	return fs.rdf, nil
}

func (fs *fileStorage) closeReader() error {
	if fs.rdf == nil {
		return nil
	}
	err := fs.rdf.Close()
	fs.rdf = nil
	return err
}

type memoryStorage struct {
	data []byte
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		data: make([]byte, 0),
	}
}

// Write implements io.Writer
func (ms *memoryStorage) Write(p []byte) (n int, err error) {
	ms.data = append(ms.data, p...)
	return len(p), nil
}

func (ms *memoryStorage) size() int {
	return len(ms.data)
}

func (ms *memoryStorage) writer() (io.Writer, error) {
	return ms, nil
}

func (ms *memoryStorage) closeWriter() error {
	return nil
}

func (ms *memoryStorage) reader(position int) (io.Reader, error) {
	if position < 0 {
		panic("negative file position")
	}
	if position >= len(ms.data) {
		return nil, fmt.Errorf("%w: position is greater than storage size", ErrSeek)
	}
	return bytes.NewReader(ms.data[position:]), nil
}

func (ms *memoryStorage) closeReader() error {
	return nil
}

func checkOrCreateDir(path string) error {
	dir, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, 0755)
			if err != nil {
				return fmt.Errorf("%w: %s", ErrCreateDir, err.Error())
			}
		}
		return nil
	}
	if !dir.IsDir() {
		return ErrPathIsFile
	}
	return nil
}

func readConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrConfigAbsent
		}
		return nil, fmt.Errorf("%w: %s", ErrReadConfig, err.Error())
	}
	defer f.Close()

	decoder := gob.NewDecoder(f)
	var c config
	if err := decoder.Decode(&c); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrReadConfig, err.Error())
	}
	return &c, nil
}

func saveConfig(path string, c *config) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("%w: %s %s", ErrCreateFile, err.Error(), path)
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(c); err != nil {
		return fmt.Errorf("%w: %s", ErrWriteConfig, err.Error())
	}
	return nil

}
