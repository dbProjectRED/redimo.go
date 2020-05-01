package redimo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicSortedSets(t *testing.T) {
	c := newClient(t)

	count, err := c.ZADD("z1", map[string]float64{"m1": 1, "m2": 2, "m3": 3, "m4": math.Inf(+1)}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	score, ok, err := c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, float64(2), score)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	_, ok, err = c.ZSCORE("z1", "nosuchmember")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err = c.ZCOUNT("z1", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	count, err = c.ZCOUNT("z1", 2, math.Inf(+1))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	count, err = c.ZCOUNT("z1", math.Inf(-1), math.Inf(+1))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	newScore, err := c.ZINCRBY("z1", "m2", 0.5)
	assert.NoError(t, err)
	assert.InDelta(t, 2.5, newScore, 0.001)

	score, ok, err = c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 2.5, score)

	newScore, err = c.ZINCRBY("zNew", "mNew", 0.5)
	assert.NoError(t, err)
	assert.InDelta(t, 0.5, newScore, 0.001)

	score, ok, err = c.ZSCORE("zNew", "mNew")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 0.5, score)
}
