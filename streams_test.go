package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamCRU(t *testing.T) {
	c := newClient(t)

	insertID1, err := c.XADD("x1", XAutoID, map[string]string{"f1": "v1", "f2": "v2"})
	assert.NoError(t, err)
	assert.Greater(t, insertID1.String(), XStart.String())

	items, err := c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID1, items[0].ID)

	count, err := c.XLEN("x1", XStart, XEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	insertID2, err := c.XADD("x1", XAutoID, map[string]string{"f3": "v3", "f4": "v4"})
	assert.NoError(t, err)
	assert.Greater(t, insertID2.String(), insertID1.String())

	count, err = c.XLEN("x1", XStart, XEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	items, err = c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID1, items[0].ID)
	assert.Equal(t, insertID2, items[1].ID)

	items, err = c.XREAD("x1", XStart, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID1, items[0].ID)

	items, err = c.XREAD("x1", items[0].ID, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID2, items[0].ID)
}
