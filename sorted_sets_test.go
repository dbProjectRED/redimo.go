package redimo

import (
	"math"
	"math/big"
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

	count, err = c.ZREM("z1", "m2", "m3", "nosuchmember")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	_, ok, err = c.ZSCORE("z1", "nosuchmember")
	assert.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = c.ZSCORE("z1", "m3")
	assert.NoError(t, err)
	assert.False(t, ok)

	newScore, err = c.ZINCRBY("zNew", "mNew", 0.5)
	assert.NoError(t, err)
	assert.InDelta(t, 0.5, newScore, 0.001)

	score, ok, err = c.ZSCORE("zNew", "mNew")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 0.5, score)
}

func TestSortedSetPops(t *testing.T) {
	c := newClient(t)

	count, err := c.ZADD("z1", map[string]float64{
		"m1": 1,
		"m2": 2,
		"m3": 3,
		"m4": 4,
		"m5": 5,
		"m6": 6,
		"m7": 7,
		"m8": 8,
		"m9": 9,
	}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count)

	membersWithScores, err := c.ZPOPMAX("z1", 2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m9": 9, "m8": 8}, membersWithScores)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(7), count)

	_, ok, err := c.ZSCORE("z1", "m9")
	assert.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = c.ZSCORE("z1", "m8")
	assert.NoError(t, err)
	assert.False(t, ok)

	membersWithScores, err = c.ZPOPMIN("z1", 2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2}, membersWithScores)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)

	_, ok, err = c.ZSCORE("z1", "m1")
	assert.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.False(t, ok)

	membersWithScores, err = c.ZPOPMIN("z1", 1000)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 3, "m4": 4, "m5": 5, "m6": 6, "m7": 7}, membersWithScores)

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestSortedSetRanges(t *testing.T) {
	c := newClient(t)

	fullSet := map[string]float64{
		"m1": 1,
		"m2": 2,
		"m3": 3,
		"m4": 4,
		"m5": 5,
		"m6": 6,
		"m7": 7,
		"m8": 8,
		"m9": 9,
	}
	count, err := c.ZADD("z1", fullSet, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count)

	fullScore, ok, err := c._zFullScoreByRank("z1", 0, true)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(1)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", 0, false)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(9)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", 5, true)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(6)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", 5, false)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(4)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", -1, true)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(9)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", -5, true)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(5)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", -5, false)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(5)), fullScore)

	fullScore, ok, err = c._zFullScoreByRank("z1", -1, true)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, floatToLex(big.NewFloat(9)), fullScore)

	set, err := c.ZRANGE("z1", 0, 3)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 3, "m4": 4}, set)

	set, err = c.ZRANGE("z1", 2, -4)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 3, "m4": 4, "m5": 5, "m6": 6}, set)

	set, err = c.ZRANGE("z1", -4, -1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m6": 6, "m7": 7, "m8": 8, "m9": 9}, set)

	set, err = c.ZREVRANGE("z1", 0, 3)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m6": 6, "m7": 7, "m8": 8, "m9": 9}, set)

	set, err = c.ZREVRANGE("z1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, fullSet, set)
}
