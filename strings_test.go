package redimo

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	c := newClient(t)
	val, err := c.GET("hello")
	assert.NoError(t, err)
	assert.Nil(t, val)

	ok, err := c.SET("hello", StringValue{"world"}, Flags{})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hello")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	str, ok := val.AsString()
	assert.True(t, ok)
	assert.Equal(t, "world", str)

	ok, err = c.SETNX("hello", NumericValue{new(big.Float).SetInt64(42)})
	assert.False(t, ok)
	assert.NoError(t, err)

	ok, err = c.SETNX("hola", NumericValue{new(big.Float).SetInt64(42)})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hola")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	n, ok := val.AsNumeric()
	assert.True(t, ok)
	assert.Equal(t, new(big.Float).SetInt64(42), n)

	ok, err = c.SET("howdy", StringValue{"partner"}, Flags{IfAlreadyExists})
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = c.SET("hola", StringValue{"mundo"}, Flags{IfAlreadyExists})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, err = c.GET("hola")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	str, ok = val.AsString()
	assert.True(t, ok)
	assert.Equal(t, "mundo", str)
}

func TestGETSET(t *testing.T) {
	c := newClient(t)
	oldValue, err := c.GETSET("hello", StringValue{"world"})
	assert.NoError(t, err)
	assert.Nil(t, oldValue)

	oldValue, err = c.GETSET("hello", StringValue{"mundo"})
	assert.NoError(t, err)
	assert.NotNil(t, oldValue)
	str, ok := oldValue.AsString()
	assert.True(t, ok)
	assert.Equal(t, "world", str)

	val, _ := c.GET("hello")
	str, _ = val.AsString()
	assert.Equal(t, "mundo", str)
}

func TestCounters(t *testing.T) {
	c := newClient(t)
	count, err := c.INCR("count")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count.Int64())

	count, err = c.INCRBY("count", big.NewInt(42))
	assert.NoError(t, err)
	assert.Equal(t, int64(43), count.Int64())

	count, err = c.DECR("count")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), count.Int64())

	count, err = c.DECRBY("count", big.NewInt(22))
	assert.NoError(t, err)
	assert.Equal(t, int64(20), count.Int64())

	num, err := c.INCRBYFLOAT("count", big.NewFloat(3.14))
	assert.NoError(t, err)

	f, _ := num.Float64()
	assert.InDelta(t, 23.14, f, 0.01)

	num, err = c.INCRBYFLOAT("count", big.NewFloat(-3.14))
	assert.NoError(t, err)

	f, _ = num.Float64()
	assert.InDelta(t, 20, f, 0.01)

	v, err := c.GET("count")
	assert.NoError(t, err)

	numeric, ok := v.AsNumeric()
	assert.True(t, ok)

	f, _ = numeric.Float64()
	assert.InDelta(t, 20, f, 0.001)
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
	assert.Equal(t, []Value{StringValue{"v1"}, StringValue{"v2"}, StringValue{"v3"}}, values)

	err = c.MSET(map[string]Value{"k3": StringValue{"v3.1"}, "k4": StringValue{"v4"}})
	assert.NoError(t, err)

	v, err := c.GET("k3")
	assert.NoError(t, err)
	assert.Equal(t, StringValue{"v3.1"}, v)

	values, err = c.MGET("k3", "k4")
	assert.NoError(t, err)
	assert.Equal(t, []Value{StringValue{"v3.1"}, StringValue{"v4"}}, values)

	ok, err := c.MSETNX(map[string]Value{"k3": StringValue{"v3.2"}, "k5": StringValue{"v5"}})
	assert.NoError(t, err)
	assert.False(t, ok)

	values, err = c.MGET([]string{"k3", "k5"}...)
	assert.Equal(t, []Value{StringValue{"v3.1"}, nil}, values)
	assert.NoError(t, err)

	ok, err = c.MSETNX(map[string]Value{"k5": StringValue{"v5"}, "k6": StringValue{"v6"}})
	assert.NoError(t, err)
	assert.True(t, ok)

	values, err = c.MGET("k5", "k6")
	assert.NoError(t, err)
	assert.Equal(t, []Value{StringValue{"v5"}, StringValue{"v6"}}, values)
}
