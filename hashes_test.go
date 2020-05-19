package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicHashes(t *testing.T) {
	c := newClient(t)
	savedFields, err := c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(savedFields))

	savedFields, err = c.HSET("k1", map[string]Value{"f1": StringValue{"v1"}, "f2": StringValue{"v2"}})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(savedFields))
	assert.Equal(t, savedFields["f2"], StringValue{"v2"})

	val, err := c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val.String())

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val.String())

	exists, err := c.HEXISTS("k1", "f2")
	assert.NoError(t, err)
	assert.True(t, exists)

	val, err = c.HGET("nosuchkey", "no such field")
	assert.NoError(t, err)
	assert.True(t, val.Empty())

	keyValues, err := c.HGETALL("k1")
	assert.NoError(t, err)
	assert.Len(t, keyValues, 2)
	assert.Equal(t, map[string]ReturnValue{"f1": {StringValue{"v1"}.ToAV()}, "f2": {StringValue{"v2"}.ToAV()}}, keyValues)

	keys, err := c.HKEYS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"f1", "f2"}, keys)

	vals, err := c.HVALS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []ReturnValue{{StringValue{"v1"}.ToAV()}, {StringValue{"v2"}.ToAV()}}, vals)

	count, err := c.HLEN("k1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	deletedFields, err := c.HDEL("k1", "f2", "f1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(deletedFields))
	assert.ElementsMatch(t, []string{"f1", "f2"}, deletedFields)

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.True(t, val.Empty())

	val, err = c.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.True(t, val.Empty())

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
	assert.Equal(t, "v1", val.String())

	val, err = c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val.String())

	values, err := c.HMGET("k1", "f1", "f2", "nonexistent")
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, "v1", values["f1"].String())
	assert.Equal(t, "v2", values["f2"].String())
	assert.False(t, values["nonexistent"].Present())

	ok, err := c.HSETNX("k1", "f1", StringValue{"v1"})
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = c.HSETNX("k1", "f9", StringValue{"v9"})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.HGET("k1", "f9")
	assert.NoError(t, err)
	assert.Equal(t, "v9", val.String())
}

func TestHashCounters(t *testing.T) {
	c := newClient(t)

	after, err := c.HINCRBYFLOAT("k1", "f1", 3.14)
	assert.NoError(t, err)
	assert.InDelta(t, 3.14, after, 0.001)

	after, err = c.HINCRBYFLOAT("k1", "f1", -1.618)
	assert.NoError(t, err)
	assert.InDelta(t, 1.522, after, 0.001)

	afterInt, err := c.HINCRBY("k1", "f1", 42)
	assert.NoError(t, err)
	assert.Equal(t, int64(43), afterInt)

	afterInt, err = c.HINCRBY("k1", "f1", -13)
	assert.NoError(t, err)
	assert.Equal(t, int64(30), afterInt)

	afterInt, err = c.HINCRBY("k1", "f2", 42)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), afterInt)

	v, err := c.HGET("k1", "f2")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), v.Int())
}
