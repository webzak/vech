package vech

import (
	"reflect"
	"testing"
)

type testdata struct {
	vector []float32
	data   []byte
}

var chunks = []testdata{
	{[]float32{0.1, 0.2, 0.3, 0.4}, []byte{1, 2, 3, 4}},
	{[]float32{0.6, 0.2, 0.11, 0.66}, []byte{5, 6, 7, 8, 9, 10}},
	{[]float32{0.3, 0.9, 0.73, 0.24}, []byte{11, 12}},
	{[]float32{0.5, 0.4, 0.3, 0.2}, []byte{13, 14, 15, 16, 17}},
}

func addChunks(c *Collection, chunks []testdata) error {
	for _, d := range chunks {
		if err := c.Add(d.vector, d.data); err != nil {
			return err
		}
	}
	return nil
}

func TestCollectionMemory(t *testing.T) {
	opt := CreateDbOptions{
		VectorSize:  4,
		StorageType: Memory,
	}
	db, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
	err = addChunks(c, chunks)
	if err != nil {
		t.Fatal(err)
	}

	ln := c.Len()
	if ln != 4 {
		t.Fatalf("collection length expected to be 4, actual: %d", ln)
	}
	order := []int{2, 1, 3, 0}
	for _, n := range order {
		idxrec, err := c.Index(n)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(chunks[n].vector, idxrec.Vector) {
			t.Fatalf("vector %d read %v does not match to original: %v", n, idxrec.Vector, chunks[n].vector)
		}
		data, err := c.Data(idxrec.Position, idxrec.Size)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(chunks[n].data, data) {
			t.Fatalf("Data %d read %v does not match to original: %v", n, data, chunks[n].data)
		}
	}
}

func TestCollectionFileSystem(t *testing.T) {
	path, err := setupDir("testdb")
	if err != nil {
		t.Fatal(err)
	}
	opt := CreateDbOptions{
		VectorSize:  4,
		StorageType: FileSystem,
		Path:        path,
	}
	db, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
	err = addChunks(c, chunks)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	c.Close()

	// reopen for read

	c, err = db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
	ln := c.Len()
	if ln != 4 {
		t.Fatalf("collection length expected to be 4, actual: %d", ln)
	}
	order := []int{2, 1, 3, 0}
	for _, n := range order {
		idxrec, err := c.Index(n)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(chunks[n].vector, idxrec.Vector) {
			t.Fatalf("vector %d read %v does not match to original: %v", n, idxrec.Vector, chunks[n].vector)
		}
		data, err := c.Data(idxrec.Position, idxrec.Size)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(chunks[n].data, data) {
			t.Fatalf("Data %d read %v does not match to original: %v", n, data, chunks[n].data)
		}
	}
}
