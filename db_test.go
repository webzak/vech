package vech

import "testing"

func TestCreateDb(t *testing.T) {
	opt := CreateDbOptions{
		VectorSize:  16,
		StorageType: Memory,
	}
	_, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	opt = CreateDbOptions{
		VectorSize:  16,
		StorageType: FileSystem,
		Path:        path,
	}
	_, err = CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenFileDb(t *testing.T) {
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	opt := CreateDbOptions{
		VectorSize:  16,
		StorageType: FileSystem,
		Path:        path,
	}
	_, err = CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenFileDb(path)
	if err != nil {
		t.Fatal(err)
	}
	if opt.VectorSize != db.config.VectorSize {
		t.Fatal("config load error")
	}
}

func TestOpenCollectionMemory(t *testing.T) {
	opt := CreateDbOptions{
		VectorSize:  16,
		StorageType: Memory,
	}
	db, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenCollectionMFile(t *testing.T) {
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	opt := CreateDbOptions{
		VectorSize:  16,
		StorageType: FileSystem,
		Path:        path,
	}
	db, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
}
