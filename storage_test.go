package vech

import (
	"errors"
	"io"
	"testing"
)

func TestMemoryStorage(t *testing.T) {
	ms := newMemoryStorage()
	size := ms.size()
	if size != 0 {
		t.Fatal("size expected to be zero on new memory storage")
	}

	_, err := ms.reader(0)
	if err == nil || !errors.Is(err, ErrSeek) {
		t.Fatalf("error expected to be ErrFileSeek, returned: %v", err)
	}

	writer, err := ms.writer()
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	n, err = writer.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	size = ms.size()
	if size != 32 {
		t.Fatalf("size expected to be 32, actual: %d", size)
	}
	rb := make([]byte, 64)
	reader, err := ms.reader(14)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	n, err = reader.Read(rb)
	if err != nil {
		t.Fatalf("error expected to be nil, returned %v", err)
	}
	if n != 18 {
		t.Fatalf("expected to read 18 bytes, actual: %d", n)
	}
	n, err = reader.Read(rb)
	if err != io.EOF {
		t.Fatalf("error expected to be io.EOF, returned %v", err)
	}
	if n != 0 {
		t.Fatalf("expected to read 0 bytes, actual: %d", n)
	}
	if rb[0] != 14 {
		t.Fatalf("expected value 14, actual: %d", rb[0])
	}
	if rb[17] != 15 {
		t.Fatalf("expected value 14, actual: %d", rb[17])
	}
	_, err = ms.reader(33)
	if err == nil || !errors.Is(err, ErrSeek) {
		t.Fatalf("error expected to be ErrFileSeek, returned: %v", err)
	}
}

func TestFileStorage(t *testing.T) {
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := openFileStorage(path + "/storage.bin")
	if err != nil {
		t.Fatal(err)
	}
	size := fs.size()
	if size != 0 {
		t.Fatal("size expected to be zero on new memory storage")
	}

	_, err = fs.reader(0)
	if err == nil || !errors.Is(err, ErrSeek) {
		t.Fatalf("error expected to be ErrFileSeek, returned: %v", err)
	}

	writer, err := fs.writer()
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	n, err = writer.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	err = fs.closeWriter()
	if err != nil {
		t.Fatal(err)
	}
	size = fs.size()
	if size != 32 {
		t.Fatalf("size expected to be 32, actual: %d", size)
	}
	rb := make([]byte, 64)
	reader, err := fs.reader(14)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	n, err = reader.Read(rb)
	if err != nil {
		t.Fatalf("error expected to be nil, returned %v", err)
	}
	if n != 18 {
		t.Fatalf("expected to read 18 bytes, actual: %d", n)
	}
	n, err = reader.Read(rb)
	if err != io.EOF {
		t.Fatalf("error expected to be io.EOF, returned %v", err)
	}
	if n != 0 {
		t.Fatalf("expected to read 0 bytes, actual: %d", n)
	}
	if rb[0] != 14 {
		t.Fatalf("expected value 14, actual: %d", rb[0])
	}
	if rb[17] != 15 {
		t.Fatalf("expected value 14, actual: %d", rb[17])
	}
	err = fs.closeReader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = fs.reader(333)
	if err == nil || !errors.Is(err, ErrSeek) {
		t.Fatalf("error expected to be ErrFileSeek, returned: %v", err)
	}
}

func TestCheckOrCreateDir(t *testing.T) {
	removeDir("testdb")
	dir, err := dirFullPath("testdb")
	if err != nil {
		t.Fatal(err)
	}
	err = checkOrCreateDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	err = checkOrCreateDir(dir)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWriteConfig(t *testing.T) {
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	c := config{
		VectorSize: 728,
	}
	cfgPath := path + "/vech"
	err = saveConfig(cfgPath, &c)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := readConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if c.VectorSize != rc.VectorSize {
		t.Fatal("config read does not match config write")
	}
}
