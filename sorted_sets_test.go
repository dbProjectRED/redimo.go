package redimo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZBasic(t *testing.T) {
	c := newClient(t)

	addedMembers, err := c.ZADD("z1", map[string]float64{"m1": 1, "m4": 4}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(addedMembers))
	assert.ElementsMatch(t, []string{"m1", "m4"}, addedMembers)

	addedMembers, err = c.ZADD("z1", map[string]float64{"m1": 1, "m2": 2, "m3": 3, "m4": 4}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(addedMembers))
	assert.ElementsMatch(t, []string{"m2", "m3"}, addedMembers)

	score, ok, err := c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, float64(2), score)

	count, err := c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	_, ok, err = c.ZSCORE("z1", "nosuchmember")
	assert.NoError(t, err)
	assert.False(t, ok)

	newScore, err := c.ZINCRBY("z1", "m2", 0.5)
	assert.NoError(t, err)
	assert.InDelta(t, 2.5, newScore, 0.001)

	score, ok, err = c.ZSCORE("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 2.5, score)

	removedMembers, err := c.ZREM("z1", "m2", "m3", "nosuchmember")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(removedMembers))
	assert.ElementsMatch(t, []string{"m2", "m3"}, removedMembers)

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

func TestZPops(t *testing.T) {
	c := newClient(t)

	addedMembers, err := c.ZADD("z1", map[string]float64{
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
	assert.Equal(t, 9, len(addedMembers))

	count, err := c.ZCARD("z1")
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

func TestZRanges(t *testing.T) {
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
	addedMembers, err := c.ZADD("z1", fullSet, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, 9, len(addedMembers))

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

	set, err = c.ZRANGEBYLEX("z1", "m2", "m6", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6}, set)

	set, err = c.ZREVRANGEBYLEX("z1", "m8", "m5", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m8": 8, "m7": 7, "m6": 6, "m5": 5}, set)

	set, err = c.ZRANGEBYSCORE("z1", 2, 6, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6}, set)

	set, err = c.ZREVRANGEBYSCORE("z1", 8, 5, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m8": 8, "m7": 7, "m6": 6, "m5": 5}, set)

	set, err = c.ZRANGEBYLEX("z1", "m2", "m6", 2, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m4": 4, "m5": 5, "m6": 6}, set)

	set, err = c.ZREVRANGEBYSCORE("z1", 8, 5, 3, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m5": 5}, set)

	set, err = c.ZREVRANGEBYLEX("z1", "m8", "m5", 0, 3)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m8": 8, "m7": 7, "m6": 6}, set)

	set, err = c.ZRANGEBYLEX("z1", "m2", "m6", 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m4": 4, "m5": 5}, set)

	set, err = c.ZRANGEBYSCORE("z1", math.Inf(-1), 3, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 3}, set)

	set, err = c.ZREVRANGEBYLEX("z1", "m3", "", 0, 3)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 3, "m2": 2, "m1": 1}, set)

	set, err = c.ZREVRANGEBYLEX("z1", "m3", "", 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 3}, set)

	set, err = c.ZREVRANGEBYLEX("z1", "m3", "", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m2": 2}, set)

	removedMembers, err := c.ZREMRANGEBYLEX("z1", "m1", "m3")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2", "m3"}, removedMembers)

	set, err = c.ZRANGEBYLEX("z1", "m1", "m3", 0, 0)
	assert.NoError(t, err)
	assert.Empty(t, set)

	count, err := c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)

	assertAbsence(t, c, "m1", "m2", "m3")

	removedMembers, err = c.ZREMRANGEBYRANK("z1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(removedMembers))
	assert.ElementsMatch(t, []string{"m4", "m5", "m6"}, removedMembers)

	assertAbsence(t, c, "m4", "m5", "m6")

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	removedMembers, err = c.ZREMRANGEBYSCORE("z1", 7, 9)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m7", "m8", "m9"}, removedMembers)

	assertAbsence(t, c, "m7", "m8", "m9")

	count, err = c.ZCARD("z1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func assertAbsence(t *testing.T, c Client, members ...string) {
	for _, member := range members {
		_, ok, err := c.ZSCORE("z1", member)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestZCounts(t *testing.T) {
	c := newClient(t)

	addedMembers, err := c.ZADD("z1", map[string]float64{
		"m1": 1,
		"m2": 2,
		"m3": 3,
		"m4": 4,
	}, Flags{})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(addedMembers))

	count, err := c.ZCOUNT("z1", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	count, err = c.ZCOUNT("z1", 2, math.Inf(+1))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	count, err = c.ZCOUNT("z1", math.Inf(-1), math.Inf(+1))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	count, err = c.ZLEXCOUNT("z1", "m2", "m4")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	rank, ok, err := c.ZRANK("z1", "m2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(1), rank)

	rank, ok, err = c.ZRANK("z1", "m4")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(3), rank)

	rank, ok, err = c.ZREVRANK("z1", "m4")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(0), rank)
}

func TestZAggregations(t *testing.T) {
	c := newClient(t)
	_, err := c.ZADD("z1", map[string]float64{
		"m1": 1,
		"m2": 2,
		"m3": 3,
	}, Flags{})
	assert.NoError(t, err)
	_, err = c.ZADD("z2", map[string]float64{
		"m3": 3.5,
		"m4": 4,
		"m5": 5,
	}, Flags{})
	assert.NoError(t, err)
	_, err = c.ZADD("z3", map[string]float64{
		"m5": 5.5,
		"m6": 6,
		"m7": 7,
	}, Flags{})
	assert.NoError(t, err)

	set, err := c.ZUNION([]string{"z1", "z2", "z3"}, ZAggregationSum, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 6.5, "m4": 4, "m5": 10.5, "m6": 6, "m7": 7}, set)

	set, err = c.ZUNION([]string{"z1", "z2", "z3"}, ZAggregationSum, map[string]float64{"z3": 2})
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 6.5, "m4": 4, "m5": 16, "m6": 12, "m7": 14}, set)

	set, err = c.ZUNION([]string{"z1", "z2", "z3"}, ZAggregationMin, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 3, "m4": 4, "m5": 5, "m6": 6, "m7": 7}, set)

	set, err = c.ZUNION([]string{"z1", "z2", "z3"}, ZAggregationMax, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 3.5, "m4": 4, "m5": 5.5, "m6": 6, "m7": 7}, set)

	set, err = c.ZUNION([]string{"z1", "z2", "z3"}, ZAggregationMax, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 7, "m4": 8, "m5": 10, "m6": 6, "m7": 7}, set)

	set, err = c.ZUNIONSTORE("union1", []string{"z1", "z2", "z3"}, ZAggregationMax, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, 7, len(set))

	set, err = c.ZRANGEBYSCORE("union1", math.Inf(-1), math.Inf(+1), 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m1": 1, "m2": 2, "m3": 7, "m4": 8, "m5": 10, "m6": 6, "m7": 7}, set)

	set, err = c.ZINTER([]string{"z1", "z3"}, ZAggregationSum, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{}, set)

	set, err = c.ZINTER([]string{"z1", "z2"}, ZAggregationSum, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 6.5}, set)

	set, err = c.ZINTER([]string{"z1", "z2"}, ZAggregationSum, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 10}, set)

	set, err = c.ZINTER([]string{"z1", "z2"}, ZAggregationMin, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 3}, set)

	set, err = c.ZINTER([]string{"z1", "z2"}, ZAggregationMax, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 7}, set)

	set, err = c.ZINTERSTORE("inter1", []string{"z1", "z2"}, ZAggregationMax, map[string]float64{"z2": 2})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(set))

	set, err = c.ZRANGEBYSCORE("inter1", math.Inf(-1), math.Inf(+1), 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, map[string]float64{"m3": 7}, set)
}
