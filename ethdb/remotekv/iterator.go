package remotekv

import (
	"github.com/ethereum/go-ethereum/ethdb"

	api "github.cbhq.net/dinh-nguyen/kvdb/gen/go/coinbase/kvdb/api/v1"
)

type iterator struct {
	kvs   []*api.ReadRange
	index int
	err   error
}

var _ ethdb.Iterator = (*iterator)(nil)

func newEmptyIterator(err error) *iterator {
	return &iterator{
		index: -1,
		err:   err,
	}
}

func newIterator(kvs []*api.ReadRange) *iterator {
	return &iterator{
		kvs:   kvs,
		index: -1,
	}
}

// Error implements ethdb.Iterator.
func (i *iterator) Error() error {
	return i.err
}

// Key implements ethdb.Iterator.
func (i *iterator) Key() []byte {
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	return i.kvs[i.index].Key
}

// Next implements ethdb.Iterator.
func (i *iterator) Next() bool {
	if i.index >= len(i.kvs) {
		return false
	}
	i.index++
	return i.index < len(i.kvs)
}

// Release implements ethdb.Iterator.
func (i *iterator) Release() {
	i.kvs = nil
	i.index = -1
}

// Value implements ethdb.Iterator.
func (i *iterator) Value() []byte {
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	return i.kvs[i.index].Val
}
