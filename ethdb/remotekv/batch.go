package remotekv

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

type batch struct {
	db     *Database
	writes []keyvalue
	size   int
}

var _ ethdb.Batch = (*batch)(nil)

// Delete implements ethdb.Batch.
func (b *batch) Delete(key []byte) error {
	b.writes = append(b.writes,
		keyvalue{key, nil, true},
	)
	b.size += len(key)
}

// Put implements ethdb.Batch.
func (b *batch) Put(key []byte, value []byte) error {
	b.writes = append(b.writes,
		keyvalue{key, common.CopyBytes(value), false},
	)
	b.size += len(key) + len(value)

	return nil
}

// Replay implements ethdb.Batch.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	panic("unimplemented")
}

// Reset implements ethdb.Batch.
func (b *batch) Reset() {
	b.writes = b.writes[:0]
	b.size = 0
}

// ValueSize implements ethdb.Batch.
func (b *batch) ValueSize() int {
	return b.size
}

// Write implements ethdb.Batch.
func (b *batch) Write() error {
	panic("unimplemented")
}
