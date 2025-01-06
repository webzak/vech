package vech

import (
	"errors"
)

var (
	ErrCorruptedDb     = errors.New("corrupted database error")
	ErrIndexOutOfRange = errors.New("index out of range")
	ErrDataPosition    = errors.New("data position is not wrong")
	ErrReadData        = errors.New("error reading data")
)

// IndexRecord represents the data containing in the index
type IndexRecord struct {
	Position int       // data position from the start of the storage
	Size     int       // data length
	Vector   []float32 // vector
}

// Collection represents single collection
type Collection struct {
	indexStorage storage
	dataStorage  storage
	vectorSize   int
	recordSize   int
	dataSize     int
	index        []byte
}

// Len returns amount of records in collection
func (c *Collection) Len() int {
	return len(c.index) / c.recordSize
}

func (c *Collection) Add(vector []float32, data []byte) error {
	ln := len(c.index)
	end := ln + c.recordSize
	vecbytes := float32SliceToByte(vector)
	dataStart := c.dataSize
	dataLen := len(data)

	if cap(c.index) >= end {
		c.index = c.index[:end] //expand without copy/alloc
		intToBytes(dataStart, c.index[ln:])
		intToBytes(dataLen, c.index[ln+8:])
		copy(c.index[ln+16:], vecbytes)
	} else {
		head := make([]byte, 16)
		intToBytes(c.dataSize, head)
		intToBytes(len(data), head[8:])
		c.index = append(c.index, head...)
		c.index = append(c.index, vecbytes...)
	}
	idxWriter, err := c.indexStorage.writer()
	if err != nil {
		return err
	}
	if _, err = idxWriter.Write(c.index[ln:]); err != nil {
		return err
	}
	dataWriter, err := c.dataStorage.writer()
	if err != nil {
		return err
	}
	if _, err = dataWriter.Write(data); err != nil {
		return err
	}
	c.dataSize += dataLen
	return nil
}

func (c *Collection) Index(n int) (*IndexRecord, error) {
	if n < 0 {
		return nil, ErrIndexOutOfRange
	}
	var ret IndexRecord

	start := c.recordSize * n
	end := start + 16
	if end > len(c.index) {
		return nil, ErrIndexOutOfRange
	}
	ret.Position = bytesToInt(c.index[start : start+8])
	ret.Size = bytesToInt(c.index[start+8 : start+16])

	start = end
	end = start + c.vectorSize*4
	if end > len(c.index) {
		return nil, ErrIndexOutOfRange
	}
	ret.Vector = bytesToFloat32Slice(c.index[start:end])
	return &ret, nil
}

func (c *Collection) Data(pos, size int) ([]byte, error) {
	if pos < 0 || size <= 0 || size >= c.dataStorage.size() {
		return nil, ErrDataPosition
	}
	reader, err := c.dataStorage.reader(pos)
	if err != nil {
		return nil, err
	}
	out := make([]byte, size)
	cnt, err := reader.Read(out)
	if err != nil {
		return nil, err
	}
	if cnt != size {
		return nil, ErrReadData
	}
	return out, nil
}

func (c *Collection) Close() error {
	var errs []error
	if err := c.indexStorage.closeReader(); err != nil {
		errs = append(errs, err)
	}
	if err := c.indexStorage.closeWriter(); err != nil {
		errs = append(errs, err)
	}
	if err := c.dataStorage.closeReader(); err != nil {
		errs = append(errs, err)
	}
	if err := c.dataStorage.closeWriter(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
