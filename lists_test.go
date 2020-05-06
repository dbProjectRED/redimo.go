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

	_, err = c.RPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	_, err = c.LPUSHX("nonexistentlist", "car")
	assert.NoError(t, err)

	elements, err = c.LRANGE("nonexistentlist", 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, elements)
}
