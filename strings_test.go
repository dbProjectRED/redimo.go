package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	c := newClient(t)
	val, err := c.GET("hello")
	assert.NoError(t, err)
	assert.False(t, val.Present())

	ok, err := c.SET("hello", StringValue{"world"}, Unconditionally)
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hello")
	assert.NoError(t, err)
	assert.True(t, val.Present())
	assert.Equal(t, "world", val.String())

	ok, err = c.SETNX("hello", IntValue{42})
	assert.False(t, ok)
	assert.NoError(t, err)

	ok, err = c.SETNX("hola", IntValue{42})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hola")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val.Int())

	ok, err = c.SET("howdy", StringValue{"partner"}, IfAlreadyExists)
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = c.SET("hola", StringValue{"mundo"}, IfAlreadyExists)
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hola")
	assert.NoError(t, err)
	assert.Equal(t, "mundo", val.String())
}

func TestGETSET(t *testing.T) {
	c := newClient(t)
	oldValue, err := c.GETSET("hello", StringValue{"world"})
	assert.NoError(t, err)
	assert.False(t, oldValue.Present())
	assert.True(t, oldValue.Empty())

	oldValue, err = c.GETSET("hello", StringValue{"mundo"})
	assert.NoError(t, err)
	assert.Equal(t, "world", oldValue.String())

	val, _ := c.GET("hello")
	assert.Equal(t, "mundo", val.String())
}

func TestCounters(t *testing.T) {
	c := newClient(t)
	count, err := c.INCR("count")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	count, err = c.INCRBY("count", 42)
	assert.NoError(t, err)
	assert.Equal(t, int64(43), count)

	count, err = c.DECR("count")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), count)

	count, err = c.DECRBY("count", 22)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), count)

	num, err := c.INCRBYFLOAT("count", 3.14)
	assert.NoError(t, err)
	assert.InDelta(t, 23.14, num, 0.01)

	num, err = c.INCRBYFLOAT("count", -3.14)
	assert.NoError(t, err)
	assert.InDelta(t, 20, num, 0.01)

	v, err := c.GET("count")
	assert.NoError(t, err)
	assert.InDelta(t, 20, v.Float(), 0.001)
}

func TestAtomicOps(t *testing.T) {
	c := newClient(t)
	err := c.MSET(map[string]Value{
		"k1": StringValue{"v1"},
		"k2": StringValue{"v2"},
		"k3": StringValue{"v3"},
	})
	assert.NoError(t, err)
	values, err := c.MGET([]string{"k1", "k2", "k3"}...)
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, "v1", values["k1"].String())
	assert.Equal(t, "v2", values["k2"].String())
	assert.Equal(t, "v3", values["k3"].String())

	err = c.MSET(map[string]Value{"k3": StringValue{"v3.1"}, "k4": StringValue{"v4"}})
	assert.NoError(t, err)

	v, err := c.GET("k3")
	assert.NoError(t, err)
	assert.Equal(t, "v3.1", v.String())

	values, err = c.MGET("k3", "k4")
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, "v3.1", values["k3"].String())
	assert.Equal(t, "v4", values["k4"].String())

	ok, err := c.MSETNX(map[string]Value{"k3": StringValue{"v3.2"}, "k5": StringValue{"v5"}})
	assert.NoError(t, err)
	assert.False(t, ok)

	values, err = c.MGET([]string{"k3", "k5"}...)
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, "v3.1", values["k3"].String())
	assert.False(t, values["k5"].Present())
	assert.NoError(t, err)

	ok, err = c.MSETNX(map[string]Value{"k5": StringValue{"v5"}, "k6": StringValue{"v6"}})
	assert.NoError(t, err)
	assert.True(t, ok)

	values, err = c.MGET("k5", "k6")
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, "v5", values["k5"].String())
	assert.Equal(t, "v6", values["k6"].String())
}
