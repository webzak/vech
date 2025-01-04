package vech

import (
	"encoding/binary"
	"unsafe"
)

func float32SliceToByte(floats []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&floats[0])), len(floats)*4)
}

func bytesToFloat32Slice(bytes []byte) []float32 {
	return unsafe.Slice((*float32)(unsafe.Pointer(&bytes[0])), len(bytes)/4)
}

func intToBytes(value int, dst []byte) {
	if len(dst) < 8 {
		panic("destination size does is less than integer")
	}
	binary.BigEndian.PutUint64(dst, uint64(value))
}

func bytesToInt(bytes []byte) int {
	v := binary.BigEndian.Uint64(bytes)
	return int(v)
}
