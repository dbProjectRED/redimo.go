package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicSortedSets(t *testing.T) {
	c := newClient(t)

	count, err := c.ZADD("z1", map[string]float64{"m1": 1, "m2": 2, "m3": 3}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	score, ok, err := c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, float64(2), score)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	_, ok, err = c.ZSCORE("z1", "nosuchmember")
	assert.NoError(t, err)
	assert.False(t, ok)
}
