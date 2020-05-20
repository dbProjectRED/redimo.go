package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicSets(t *testing.T) {
	c := newClient(t)

	addedMembers, err := c.SADD("s1", "m1", "m2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(addedMembers))
	assert.ElementsMatch(t, []string{"m1", "m2"}, addedMembers)

	addedMembers, err = c.SADD("s1", "m1", "m2", "m3")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(addedMembers))
	assert.ElementsMatch(t, []string{"m3"}, addedMembers)

	ok, err := c.SISMEMBER("s1", "m1")
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = c.SISMEMBER("s1", "nonexistentmember")
	assert.NoError(t, err)
	assert.False(t, ok)

	members, err := c.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2", "m3"}, members)

	count, err := c.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	members, err = c.SMEMBERS("nosuchset")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, members)

	removedMembers, err := c.SREM("s1", "m1", "m2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2"}, removedMembers)

	removedMembers, err = c.SREM("s1", "m1", "m2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, removedMembers)

	members, err = c.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m3"}, members)

	ok, err = c.SISMEMBER("s1", "m1")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err = c.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	count, err = c.SCARD("nosuchkey")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestSetOperations(t *testing.T) {
	c := newClient(t)

	_, err := c.SADD("s1", "m1", "m2", "m3")
	assert.NoError(t, err)

	_, err = c.SADD("s2", "m3", "m4", "m5")
	assert.NoError(t, err)

	_, err = c.SADD("s3", "m5", "m6", "m7")
	assert.NoError(t, err)

	union, err := c.SUNION("s1", "s2", "s3")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7"}, union)

	unionCount, err := c.SUNIONSTORE("union", "s1", "s2", "s3")
	assert.NoError(t, err)
	assert.Equal(t, int64(7), unionCount)

	unionMembers, err := c.SMEMBERS("union")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7"}, unionMembers)

	intersection, err := c.SINTER("s1", "s2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m3"}, intersection)

	intersection, err = c.SINTER("s1", "s3")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, intersection)

	intersectionCount, err := c.SINTERSTORE("inter", "s1", "s2")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), intersectionCount)

	intersectionMembers, err := c.SMEMBERS("inter")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m3"}, intersectionMembers)

	diff, err := c.SDIFF("s1", "s2", "s3")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2"}, diff)

	diff, err = c.SDIFF("s1", "s3")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2", "m3"}, diff)

	diffCount, err := c.SDIFFSTORE("diff", "s1", "s2", "s3")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), diffCount)

	diffMembers, err := c.SMEMBERS("diff")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2"}, diffMembers)

	union, err = c.SUNION()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, union)
}

func TestSetModifiers(t *testing.T) {
	c := newClient(t)

	_, err := c.SADD("s1", "m1", "m2", "m3")
	assert.NoError(t, err)

	members, err := c.SRANDMEMBER("s1", 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Subset(t, []string{"m1", "m2", "m3"}, members)

	members, err = c.SRANDMEMBER("s1", -2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Subset(t, []string{"m1", "m2", "m3"}, members)

	members, err = c.SPOP("s1", 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Subset(t, []string{"m1", "m2", "m3"}, members)

	newCount, err := c.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), newCount)

	_, err = c.SADD("s1", "m1", "m2", "m3")
	assert.NoError(t, err)

	ok, err := c.SMOVE("s1", "s2", "m1")
	assert.NoError(t, err)
	assert.True(t, ok)

	members, err = c.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m2", "m3"}, members)

	members, err = c.SMEMBERS("s2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1"}, members)

	ok, err = c.SMOVE("s1", "s2", "nosuchmember")
	assert.NoError(t, err)
	assert.False(t, ok)

	members, err = c.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m2", "m3"}, members)

	members, err = c.SMEMBERS("s2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1"}, members)
}
