package serializer

import "bytes"

func WriteString(v string) []byte {
	return []byte(v)
}

func ReadString(v []byte) string {
	n := bytes.Index(v, []byte{0})
	if n == -1 {
		return string(v)
	}
	return string(v[:n])
}

func WriteUint32(v uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((v >> (8 * i)) & 0xff)
	}
	return r
}

func ReadUint32(v [4]byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(v[i]) << (8 * i)
	}
	return r
}
