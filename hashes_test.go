package redimo

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicHashes(t *testing.T) {
	c := newClient(t)
	savedCount, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, savedCount)
	val, err := c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v1"}, val)

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v2"}, val)

	exists, err := c.HEXISTS("k1", "f2")
	assert.NoError(t, err)
	assert.True(t, exists)

	val, err = c.HGET("nosuchkey", "no such field")
	assert.NoError(t, err)
	assert.Nil(t, val)

	keyValues, err := c.HGETALL("k1")
	assert.NoError(t, err)
	assert.Equal(t, map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}}, keyValues)

	keys, err := c.HKEYS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"f1", "f2"}, keys)

	vals, err := c.HVALS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []Value{StringValue{"v1"}, StringValue{"v2"}}, vals)

	count, err := c.HLEN("k1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	err = c.HDEL("k1", "f2", "f1")
	assert.NoError(t, err)

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Nil(t, val)

	val, err = c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Nil(t, val)

	exists, err = c.HEXISTS("k1", "f1")
	assert.NoError(t, err)
	assert.False(t, exists)

	keys, err = c.HKEYS("nosuchkey")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, keys)

	vals, err = c.HVALS("nosuchkey")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []Value{}, vals)

	count, err = c.HLEN("nosuchkey")
	assert.NoError(t, err)
	assert.Zero(t, count)
}

func TestAtomicHashOps(t *testing.T) {
	c := newClient(t)
	err := c.HMSET("k1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)

	val, err := c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v1"}, val)

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v2"}, val)

	values, err := c.HMGET("k1", "f1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, []Value{StringValue{"v1"}, StringValue{"v2"}}, values)

}

func TestHashCounters(t *testing.T) {
	c := newClient(t)

	after, err := c.HINCRBYFLOAT("k1", "f1", big.NewFloat(3.14))
	assert.NoError(t, err)
	f, _ := after.Float64()
	assert.InDelta(t, 3.14, f, 0.001)

	after, err = c.HINCRBYFLOAT("k1", "f1", big.NewFloat(-1.618))
	assert.NoError(t, err)
	f, _ = after.Float64()
	assert.InDelta(t, 1.522, f, 0.001)

	afterInt, err := c.HINCRBY("k1", "f1", big.NewInt(42))
	assert.NoError(t, err)
	assert.Equal(t, int64(43), afterInt.Int64())

	afterInt, err = c.HINCRBY("k1", "f1", big.NewInt(-13))
	assert.NoError(t, err)
	assert.Equal(t, int64(30), afterInt.Int64())

	afterInt, err = c.HINCRBY("k1", "f2", big.NewInt(42))
	assert.NoError(t, err)
	assert.Equal(t, int64(42), afterInt.Int64())

	v, err := c.HGET("k1", "f2")
	assert.NoError(t, err)
	nval, ok := v.AsNumeric()
	assert.True(t, ok)
	n, _ := nval.Int64()
	assert.Equal(t, n, int64(42))
}
