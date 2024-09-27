package remotekv_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
	"github.com/ethereum/go-ethereum/ethdb/remotekv"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRemoteKVDatabase(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
			assert.NoError(t, err)
			s := remotekv.New(conn)
			err = s.Reset()
			assert.NoError(t, err)
			return s
		})
	})
}

func TestRemoteKV(t *testing.T) {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	c := remotekv.New(conn)

	t.Run("Has not found", func(t *testing.T) {
		k := []byte(uuid.New().String())

		found, err := c.Has(k)
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Put and Get", func(t *testing.T) {
		k := []byte(uuid.New().String())

		err := c.Put(k, []byte("value"))
		assert.NoError(t, err)

		value, err := c.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("Put, Delete and Get", func(t *testing.T) {
		k := []byte(uuid.New().String())

		err := c.Put(k, []byte("value"))
		assert.NoError(t, err)

		err = c.Delete(k)
		assert.NoError(t, err)

		found, err := c.Has(k)
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Batch", func(t *testing.T) {
		b := c.NewBatch()

		k1 := []byte(uuid.New().String())
		k2 := []byte(uuid.New().String())
		k3 := []byte(uuid.New().String())

		assert.NoError(t, b.Put(k1, k1))
		assert.NoError(t, b.Put(k2, k2))
		assert.NoError(t, b.Put(k3, k3))

		// no commit yet
		found, err := c.Has(k1)
		assert.NoError(t, err)
		assert.False(t, found)

		// commit
		assert.NoError(t, b.Write())

		// check
		found, err = c.Has(k1)
		assert.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("Iterator", func(t *testing.T) {
		k1 := []byte("prefix-" + "11-" + uuid.New().String())
		k2 := []byte("prefix-" + "12-" + uuid.New().String())
		k3 := []byte("prefix-" + "13-" + uuid.New().String())

		assert.NoError(t, c.Put(k1, k1))
		assert.NoError(t, c.Put(k2, k2))
		assert.NoError(t, c.Put(k3, k3))

		it := c.NewIterator([]byte("prefix-"), []byte("1"))

		assert.Nil(t, it.Key())
		assert.True(t, it.Next())
		assert.Equal(t, k1, it.Key())
		assert.Equal(t, k1, it.Value())
	})
}
