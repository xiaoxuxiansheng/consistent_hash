package consistent_hash

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type Encryptor interface {
	Encrypt(origin string) int32
}

type MurmurHasher struct {
}

func NewMurmurHasher() *MurmurHasher {
	return &MurmurHasher{}
}

func (m *MurmurHasher) Encrypt(origin string) int32 {
	hasher := murmur3.New32()
	_, _ = hasher.Write([]byte(origin))
	return int32(hasher.Sum32() % math.MaxInt32)
}
