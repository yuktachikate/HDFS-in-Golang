package filters

import (
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	numHashes uint32 //number of hash functions
	elements  uint32 //number of elements
	bits      []bool
}

// New /* initialize bloom filter */
func New(numBits, numHashes uint32) *BloomFilter {
	bits := make([]bool, numBits)
	return &BloomFilter{numHashes: numHashes, elements: 0, bits: bits}
}

/* get bits */
func (bf *BloomFilter) getBits(data []byte) []uint32 {
	sum1 := murmur3.Sum32(data)
	sum2 := murmur3.Sum32WithSeed(data, sum1)
	results := make([]uint32, bf.numHashes)
	for i := uint32(0); i < bf.numHashes; i++ {
		results[i] = (sum1 + (i * sum2)) % uint32(len(bf.bits))
	}

	return results
}

// Put /* insert data into bloom filter */
func (bf *BloomFilter) Put(data []byte) {
	arr := bf.getBits(data)
	for _, val := range arr {
		bf.bits[val] = true
	}
}

// Get /* check if data exists in bloom filter */
func (bf *BloomFilter) Get(data []byte) bool {
	arr := bf.getBits(data)
	if len(arr) == 0 {
		return false
	} else {
		result := true
		for _, val := range arr {
			result = result && bf.bits[val]
		}
		return result
	}
}
