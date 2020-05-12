package redimo

import (
	"testing"
	"time"

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
	assert.Equal(t, 2, len(items[0].Fields))
	assert.Equal(t, 2, len(items[1].Fields))
	assert.Equal(t, insertID1, items[0].ID)
	assert.Equal(t, "v1", items[0].Fields["f1"])
	assert.Equal(t, insertID2, items[1].ID)
	assert.Equal(t, "v4", items[1].Fields["f4"])

	items, err = c.XREVRANGE("x1", XEnd, XStart, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID2, items[0].ID)
	assert.Equal(t, insertID1, items[1].ID)

	items, err = c.XREAD("x1", XStart, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID1, items[0].ID)

	items, err = c.XREAD("x1", items[0].ID, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, insertID2, items[0].ID)
}

func TestStreamDeletes(t *testing.T) {
	c := newClient(t)

	insertID1, err := c.XADD("x1", XAutoID, map[string]string{"f1": "v1", "f2": "v2"})
	assert.NoError(t, err)
	insertID2, err := c.XADD("x1", XAutoID, map[string]string{"f3": "v3", "f4": "v4"})
	assert.NoError(t, err)
	insertID3, err := c.XADD("x1", XAutoID, map[string]string{"f5": "v5", "f6": "v6"})
	assert.NoError(t, err)
	insertID4, err := c.XADD("x1", XAutoID, map[string]string{"f5": "v5", "f6": "v6"})
	assert.NoError(t, err)

	items, err := c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(items))
	assert.Equal(t, insertID1, items[0].ID)
	assert.Equal(t, insertID2, items[1].ID)
	assert.Equal(t, insertID3, items[2].ID)
	assert.Equal(t, insertID4, items[3].ID)

	deletedCount, err := c.XDEL("x1", insertID2, insertID3, NewXID(time.Now(), 1234))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deletedCount)

	items, err = c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID1, items[0].ID)
	assert.Equal(t, insertID4, items[1].ID)

	insertID5, err := c.XADD("x1", XAutoID, map[string]string{"f7": "v7", "f8": "v8"})
	assert.NoError(t, err)
	items, err = c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, insertID5, items[2].ID)

	deletedCount, err = c.XTRIM("x1", items[1].ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deletedCount)

	items, err = c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID4, items[0].ID)
	assert.Equal(t, insertID5, items[1].ID)
}
