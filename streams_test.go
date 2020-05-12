package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamAddAndRange(t *testing.T) {
	c := newClient(t)

	insertID1, err := c.XADD("x1", StreamAutoID, map[string]string{"f1": "v1", "f2": "v2"})
	assert.NoError(t, err)
	assert.Greater(t, insertID1.String(), StreamStart.String())

	items, err := c.XRANGE("x1", StreamStart, StreamEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID1, items[0].ID)

	insertID2, err := c.XADD("x1", StreamAutoID, map[string]string{"f3": "v3", "f4": "v4"})
	assert.NoError(t, err)
	assert.Greater(t, insertID2.String(), insertID1.String())

	items, err = c.XRANGE("x1", StreamStart, StreamEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID1, items[0].ID)
	assert.Equal(t, insertID2, items[1].ID)
}
