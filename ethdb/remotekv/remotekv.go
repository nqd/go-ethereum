package remotekv

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"google.golang.org/grpc"

	api "github.cbhq.net/dinh-nguyen/kvdb/gen/go/coinbase/kvdb/api/v1"
)

type Database struct {
	conn   *grpc.ClientConn
	client api.KVClient
}

var _ ethdb.Database = (*Database)(nil)

var (
	errRemoteKVNotFound = errors.New("not found")
)

func New(conn *grpc.ClientConn) *Database {
	client := api.NewKVClient(conn)

	return &Database{
		conn:   conn,
		client: client,
	}
}

// Ancient implements ethdb.Database.
func (d *Database) Ancient(kind string, number uint64) ([]byte, error) {
	panic("unimplemented")
}

// AncientDatadir implements ethdb.Database.
func (d *Database) AncientDatadir() (string, error) {
	panic("unimplemented")
}

// AncientRange implements ethdb.Database.
func (d *Database) AncientRange(kind string, start uint64, count uint64, maxBytes uint64) ([][]byte, error) {
	panic("unimplemented")
}

// AncientSize implements ethdb.Database.
func (d *Database) AncientSize(kind string) (uint64, error) {
	panic("unimplemented")
}

// Ancients implements ethdb.Database.
func (d *Database) Ancients() (uint64, error) {
	panic("unimplemented")
}

// Close implements ethdb.Database.
func (d *Database) Close() error {
	return d.conn.Close()
}

// Compact implements ethdb.Database.
func (d *Database) Compact(start []byte, limit []byte) error {
	panic("unimplemented")
}

// Delete implements ethdb.Database.
func (d *Database) Delete(key []byte) error {
	panic("unimplemented")
}

// Get implements ethdb.Database.
// TODO: Use stream API
// TODO: Verify the not found case
func (d *Database) Get(key []byte) ([]byte, error) {
	res, err := d.client.Read(context.Background(), &api.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}

	if res.Val == nil {
		return nil, errRemoteKVNotFound
	}

	return res.Val, nil
}

// Has implements ethdb.Database.
func (d *Database) Has(key []byte) (bool, error) {
	panic("unimplemented")
}

// HasAncient implements ethdb.Database.
func (d *Database) HasAncient(kind string, number uint64) (bool, error) {
	panic("unimplemented")
}

// ModifyAncients implements ethdb.Database.
func (d *Database) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	panic("unimplemented")
}

// NewBatch implements ethdb.Database.
func (d *Database) NewBatch() ethdb.Batch {
	return &batch{
		db: d,
	}
}

// NewBatchWithSize implements ethdb.Database.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{
		db:     d,
		writes: make([]*api.Write, 0, size),
	}
}

// NewIterator implements ethdb.Database.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	res, err := d.client.ReadAll(
		context.Background(),
		&api.ReadAllRequest{
			Prefix: prefix,
			Start:  start,
		},
	)
	if err != nil {
		return newEmptyIterator(err)
	}

	return newIterator(res.GetReads())
}

// Put implements ethdb.Database.
func (d *Database) Put(key []byte, value []byte) error {
	panic("unimplemented")
}

// ReadAncients implements ethdb.Database.
func (d *Database) ReadAncients(fn func(ethdb.AncientReaderOp) error) (err error) {
	panic("unimplemented")
}

// Stat implements ethdb.Database.
func (d *Database) Stat() (string, error) {
	panic("unimplemented")
}

// Sync implements ethdb.Database.
func (d *Database) Sync() error {
	panic("unimplemented")
}

// Tail implements ethdb.Database.
func (d *Database) Tail() (uint64, error) {
	panic("unimplemented")
}

// TruncateHead implements ethdb.Database.
func (d *Database) TruncateHead(n uint64) (uint64, error) {
	panic("unimplemented")
}

// TruncateTail implements ethdb.Database.
func (d *Database) TruncateTail(n uint64) (uint64, error) {
	panic("unimplemented")
}
