package remotekv

import (
	"context"

	"github.com/ethereum/go-ethereum/ethdb"

	api "github.cbhq.net/dinh-nguyen/kvdb/gen/go/coinbase/kvdb/api/v1"
)

type batch struct {
	db     *Database
	writes []*api.Write
	size   int
}

var _ ethdb.Batch = (*batch)(nil)

// Delete implements ethdb.Batch.
func (b *batch) Delete(key []byte) error {
	b.writes = append(b.writes,
		&api.Write{
			Key:    key,
			Val:    nil,
			Delete: true,
		},
	)
	b.size += len(key)

	return nil
}

// Put implements ethdb.Batch.
func (b *batch) Put(key []byte, value []byte) error {
	b.writes = append(b.writes,
		&api.Write{
			Key:    key,
			Val:    value,
			Delete: false,
		},
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
	_, err := b.db.client.WriteBatch(
		context.Background(),
		&api.WriteBatchRequest{
			Writes: b.writes,
		},
	)

	return err
}
