package vech

import (
	"errors"
)

var (
	ErrCorruptedDb     = errors.New("corrupted database error")
	ErrIndexOutOfRange = errors.New("index out of range")
	ErrReadData        = errors.New("error reading data")
)

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

func (c *Collection) AddRecord(vector []float32, data []byte) error {
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
		intToBytes(c.dataSize+len(data), head[8:])
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

func (c *Collection) Vector(n int) ([]float32, error) {
	if n < 0 {
		return nil, ErrIndexOutOfRange
	}
	start := c.recordSize*n + 16
	end := start + c.vectorSize
	if end > len(c.index) {
		return nil, ErrIndexOutOfRange
	}
	return bytesToFloat32Slice(c.index[start:end]), nil
}

func (c *Collection) Data(n int) ([]byte, error) {
	if n < 0 || n >= c.Len() {
		return nil, ErrIndexOutOfRange
	}
	start := c.recordSize * n
	end := start + 16
	if end > len(c.index) {
		return nil, ErrIndexOutOfRange
	}
	pos := bytesToInt(c.index[start : start+8])
	size := bytesToInt(c.index[start+8 : start+16])

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
