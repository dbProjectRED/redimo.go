package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLBasics(t *testing.T) {
	c := newClient(t)
	length, err := c.LPUSH("l1", "inty")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)

	elements, err := c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"inty"}, elements)

	_, err = c.LPUSH("l1", "minty")
	assert.NoError(t, err)

	elements, err = c.LRANGE("l1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"minty", "inty"}, elements)
}
