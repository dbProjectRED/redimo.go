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
}
