package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicSets(t *testing.T) {
	c := newClient(t)

	err := c.SADD("s1", "m1", "m2", "m3")
	assert.NoError(t, err)

	ok, err := c.SISMEMBER("s1", "m1")
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = c.SISMEMBER("s1", "nonexistentmember")
	assert.NoError(t, err)
	assert.False(t, ok)
}
