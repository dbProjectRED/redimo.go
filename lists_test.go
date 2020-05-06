package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLBasics(t *testing.T) {
	c := newClient(t)
	length, err := c.LPUSH("l1", "twinkle")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle"}, elements)

	_, err = c.LPUSH("l1", "twinkle")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle"}, elements)

	_, err = c.RPUSH("l1", "little", "star")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "twinkle", "little", "star"}, elements)

	element, found, err := c.LPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "twinkle", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little", "star"}, elements)

	element, found, err = c.RPOP("l1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "star", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, elements)

	_, err = c.LPUSHX("l1", "wrinkle")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	_, err = c.RPUSHX("l1", "car")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little", "car"}, elements)

	elements, err = c.LRANGE("l1", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"wrinkle", "twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"twinkle", "little"}, elements)

	elements, err = c.LRANGE("l1", -2, -3)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	elements, err = c.LRANGE("l1", 3, 2)
	assert.NoError(t, err)
	assert.Empty(t, elements)

	_, err = c.RPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	_, err = c.LPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)
}

func TestRPOPLPUSH(t *testing.T) {
	c := newClient(t)

	_, err := c.RPUSH("l1", "one", "two", "three", "four")
	assert.NoError(t, err)

	_, err = c.RPUSH("l2", "five", "six", "seven", "eight")
	assert.NoError(t, err)

	element, ok, err := c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "four", element)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two", "three"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "l2")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "three", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"four", "one", "two"}, elements)

	elements, err = c.LRANGE("l2", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"three", "five", "six", "seven", "eight"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "l1")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "two", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four", "one"}, elements)

	element, ok, err = c.RPOPLPUSH("l1", "newList")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "one", element)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"two", "four"}, elements)

	elements, err = c.LRANGE("newList", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"one"}, elements)

	//c.LPOP("l1")
	//
	//elements, err = c.LRANGE("l1", 0, -1)
	//assert.NoError(t, err)
	//assert.Equal(t, []string{"one", "two"}, elements)
	//
	//element, ok, err = c.RPOPLPUSH("l1", "l1")
	//assert.NoError(t, err)
	//assert.True(t, ok)
	//assert.Equal(t, "one", element)
	//
	//elements, err = c.LRANGE("l1", 0, -1)
	//assert.NoError(t, err)
	//assert.Equal(t, []string{"two", "one"}, elements)
}
