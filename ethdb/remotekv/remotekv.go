package remotekv

import (
	"context"

	"github.com/ethereum/go-ethereum/ethdb"
	"google.golang.org/grpc"

	api "github.cbhq.net/dinh-nguyen/kvdb/gen/go/coinbase/kvdb/api/v1"
)

type Database struct {
	conn   *grpc.ClientConn
	client api.KVClient
}

var _ ethdb.KeyValueStore = (*Database)(nil)

func New(conn *grpc.ClientConn) *Database {
	client := api.NewKVClient(conn)

	return &Database{
		conn:   conn,
		client: client,
	}
}

// Close implements ethdb.Database.
func (d *Database) Close() error {
	return d.conn.Close()
}

// Compact implements ethdb.Database.
// Currently does not have compact action
func (d *Database) Compact(start []byte, limit []byte) error {
	return nil
}

// Delete implements ethdb.Database.
func (d *Database) Delete(key []byte) error {
	_, err := d.client.Write(context.Background(), &api.WriteRequest{
		Key:    key,
		Delete: true,
	})

	return err
}

// Get implements ethdb.Database.
// TODO: Use stream API
// TODO: Verify the not found case
func (d *Database) Get(key []byte) ([]byte, error) {
	res, err := d.client.Read(context.Background(), &api.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}

	return res.Val, nil
}

// Has implements ethdb.Database.
// TODO: check the not found case
func (d *Database) Has(key []byte) (bool, error) {
	res, err := d.client.Read(context.Background(), &api.ReadRequest{Key: key})
	if err != nil {
		return false, err
	}
	return res.Val != nil, nil
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
	res, err := d.client.ReadRange(
		context.Background(),
		&api.ReadRangeRequest{
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
	_, err := d.client.Write(context.Background(), &api.WriteRequest{
		Key:    key,
		Val:    value,
		Delete: false,
	})

	return err
}

// Stat implements ethdb.Database.
func (d *Database) Stat() (string, error) {
	return "", nil
}
