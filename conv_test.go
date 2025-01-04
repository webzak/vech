package vech

import "testing"

func TestVectorConvert(t *testing.T) {
	fs := []float32{0.11, 0.22, 0.33}
	bs := float32SliceToByte(fs)
	rs := bytesToFloat32Slice(bs)
	if len(rs) != 3 {
		t.Fatalf("result length expected to be 3, actual: %d", len(rs))
	}
	for i, v := range fs {
		if v != rs[i] {
			t.Fatalf("Value with index %d %f is not equal expected %f", i, rs[i], v)
		}
	}
	for i := range fs {
		faddr := &fs[i]
		baddr := &rs[i]
		if faddr != baddr {
			t.Fatalf("Value address with index %d %x is not equal expected %x", faddr, rs[i], baddr)
		}
	}
}

func TestIntConvert(t *testing.T) {
	v := 4394823094832
	bs := make([]byte, 8)
	intToBytes(v, bs)
	rs := bytesToInt(bs)
	if rs != v {
		t.Fatalf("Expected %d, result %d", v, rs)
	}
}
