package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLBasics(t *testing.T) {
	c := newClient(t)

	length, err := c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle"}, readStrings(elements))

	_, err = c.LPUSH("l1", StringValue{"twinkle"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle"}, readStrings(elements))

	_, err = c.RPUSH("l1", StringValue{"little"}, StringValue{"star"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle", "little", "star"}, readStrings(elements))

	element, found, err := c.LPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "twinkle", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little", "star"}, readStrings(elements))

	element, found, err = c.RPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "star", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	_, err = c.LPUSHX("l1", StringValue{"wrinkle"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	_, err = c.RPUSHX("l1", StringValue{"car"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little", "car"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, readStrings(elements))

	elements, err = c.LRANGE("l1", -2, -3)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	elements, err = c.LRANGE("l1", 3, 2)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	_, err = c.RPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)

	_, err = c.LPUSHX("nonexistentlist", StringValue{"car"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)
}

func readStrings(elements []ReturnValue) (strs []string) {
	for _, e := range elements {
		strs = append(strs, e.String())
	}

	return
}

func TestRPOPLPUSH(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"one"}, StringValue{"two"}, StringValue{"three"}, StringValue{"four"})
	assert.NoError(t, err)

	_, err = c.RPUSH("l2", StringValue{"five"}, StringValue{"six"}, StringValue{"seven"}, StringValue{"eight"})
	assert.NoError(t, err)

	element, ok, err := c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "four", element.String())

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two", "three"}, readStrings(elements))

	element, ok, err = c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "three", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two"}, readStrings(elements))

	elements, err = c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"three", "five", "six", "seven", "eight"}, readStrings(elements))

	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four", "one"}, readStrings(elements))

	element, ok, err = c.RPOPLPUSH("l1", "newList")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "one", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four"}, readStrings(elements))

	elements, err = c.LRANGE("newList", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one"}, readStrings(elements))

	// Two item single list rotation - they should simply switch places
	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "four", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "two"}, readStrings(elements))

	_, _, err = c.LPOP("l1")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))

	// Single element single list rotation is a no-op
	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", element.String())

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two"}, readStrings(elements))
}

func TestListIndexBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"inty"}, StringValue{"minty"}, StringValue{"papa"}, StringValue{"tinty"})
	assert.NoError(t, err)

	element, found, err := c.LINDEX("l1", 0)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "inty", element.String())

	element, found, err = c.LINDEX("l1", 3)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "tinty", element.String())

	_, found, err = c.LINDEX("l1", 4)
	assert.NoError(t, err)
	assert.False(t, found)

	_, found, err = c.LINDEX("l1", 42)
	assert.NoError(t, err)
	assert.False(t, found)

	element, found, err = c.LINDEX("l1", -1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "tinty", element.String())

	element, found, err = c.LINDEX("l1", -4)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "inty", element.String())

	_, found, err = c.LINDEX("l1", -42)
	assert.NoError(t, err)
	assert.False(t, found)

	ok, err := c.LSET("l1", 1, "monty")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, found, err = c.LINDEX("l1", 1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "monty", element.String())

	ok, err = c.LSET("l1", -2, "mama")
	assert.NoError(t, err)
	assert.True(t, ok)

	element, found, err = c.LINDEX("l1", -2)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "mama", element.String())

	ok, err = c.LSET("l1", 42, "no chance")
	assert.NoError(t, err)
	assert.False(t, ok)

	count, err := c.LLEN("l1")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"inty", "monty", "mama", "tinty"}, readStrings(elements))
}

func TestListValueBasedCRUD(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", StringValue{"beta"}, StringValue{"delta"}, StringValue{"phi"})
	assert.NoError(t, err)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, readStrings(elements))

	_, ok, err := c.LINSERT("l1", Left, StringValue{"delta"}, StringValue{"gamma"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "gamma", "delta", "phi"}, readStrings(elements))

	_, ok, err = c.LINSERT("l1", Left, StringValue{"beta"}, StringValue{"alpha"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta", "phi"}, readStrings(elements))

	_, ok, err = c.LINSERT("l1", Right, StringValue{"phi"}, StringValue{"omega"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta", "phi", "omega"}, readStrings(elements))

	_, ok, err = c.LREM("l1", Left, StringValue{"gamma"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi", "omega"}, readStrings(elements))

	_, ok, err = c.LREM("l1", Left, StringValue{"omega"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "delta", "phi"}, readStrings(elements))

	_, ok, err = c.LREM("l1", Left, StringValue{"alpha"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi"}, readStrings(elements))

	_, err = c.RPUSH("l1", StringValue{"delta"}, StringValue{"gamma"}, StringValue{"delta"}, StringValue{"mu"})
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "delta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	_, ok, err = c.LREM("l1", Left, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "delta", "mu"}, readStrings(elements))

	_, ok, err = c.LREM("l1", Right, StringValue{"delta"})
	assert.NoError(t, err)
	assert.True(t, ok)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"beta", "phi", "delta", "gamma", "mu"}, readStrings(elements))
}
