package vech

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
)

func dirFullPath(name string) (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("runtume caller path error")
	}
	fp := filepath.Dir(filename)
	return fp + "/" + name, nil
}

func createDir(name string) (string, error) {
	path, err := dirFullPath(name)
	if err != nil {
		return "", err
	}
	dir, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, 0775)
			if err != nil {
				return "", err
			}
		}
		return path, nil
	}
	if !dir.IsDir() {
		return "", errors.New(path + " is not directory")
	}
	return path, nil
}

func removeDir(name string) error {
	path, err := dirFullPath(name)
	if err != nil {
		return err
	}
	_, err = os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(path)
}

func setupDir(name string) (string, error) {
	removeDir(name)
	return createDir(name)
}
