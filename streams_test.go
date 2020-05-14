package redimo

import (
	"strconv"
	"sync"
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

	deletedCount, err = c.XTRIM("x1", 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deletedCount)

	items, err = c.XRANGE("x1", XStart, XEnd, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, insertID4, items[0].ID)
	assert.Equal(t, insertID5, items[1].ID)
}

func TestStreamsConsumerGroupsNoACK(t *testing.T) {
	c := newClient(t)
	allItems := make([]StreamItem, 0, 25)
	key := "x1"
	group := "group"

	for i := 0; i < 30; i++ {
		fields := map[string]string{"i": strconv.Itoa(i)}
		insertedID, err := c.XADD(key, XAutoID, fields)

		allItems = append(allItems, StreamItem{ID: insertedID, Fields: fields})

		assert.NoError(t, err)
	}

	err := c.XGROUP(key, group, XStart)
	assert.NoError(t, err)

	consumer1 := "mercury"
	consumer2 := "venus"
	consumer3 := "earth"

	item1, err := c.XREADGROUP(key, group, consumer1, XReadNewAutoACK, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[0], item1[0])

	item2, err := c.XREADGROUP(key, group, consumer2, XReadNewAutoACK, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[1], item2[0])

	item3, err := c.XREADGROUP(key, group, consumer3, XReadNewAutoACK, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[2], item3[0])

	collector := make(chan StreamItem)
	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			item, err := c.XREADGROUP(key, group, "parallel!", XReadNewAutoACK, 1)
			if err != nil {
				assert.NoError(t, err)
			} else {
				collector <- item[0]
			}

			wg.Add(-1)
		}()
	}

	go func() {
		wg.Wait()
		close(collector)
	}()

	parallelItems := make([]StreamItem, 0, 5)

	for item := range collector {
		parallelItems = append(parallelItems, item)
	}

	assert.ElementsMatch(t, allItems[3:8], parallelItems)
}

func TestStreamsConsumerGroupACK(t *testing.T) {
	c := newClient(t)
	allItems := make([]StreamItem, 0, 25)
	key := "x1"
	group := "group"

	for i := 0; i < 5; i++ {
		fields := map[string]string{"i": strconv.Itoa(i)}
		insertedID, err := c.XADD(key, XAutoID, fields)

		allItems = append(allItems, StreamItem{ID: insertedID, Fields: fields})

		assert.NoError(t, err)
	}

	err := c.XGROUP(key, group, XStart)
	assert.NoError(t, err)

	nowTS := time.Now().Unix()

	consumer1 := "mercury"
	consumer2 := "venus"
	consumer3 := "earth"
	item1, err := c.XREADGROUP(key, group, consumer1, XReadNew, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[0], item1[0])

	item2, err := c.XREADGROUP(key, group, consumer2, XReadNew, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[1], item2[0])

	item3, err := c.XREADGROUP(key, group, consumer3, XReadNew, 1)
	assert.NoError(t, err)
	assert.Equal(t, allItems[2], item3[0])

	pendingItems, err := c.XPENDING(key, group, 100)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(pendingItems))

	assert.Equal(t, pendingItems[0].ID, item1[0].ID)
	assert.Equal(t, pendingItems[1].ID, item2[0].ID)
	assert.Equal(t, pendingItems[2].ID, item3[0].ID)
	assert.Equal(t, consumer1, pendingItems[0].Consumer)
	assert.Equal(t, consumer2, pendingItems[1].Consumer)
	assert.Equal(t, consumer3, pendingItems[2].Consumer)
	assert.Equal(t, int64(1), pendingItems[0].DeliveryCount)
	assert.GreaterOrEqual(t, pendingItems[0].LastDelivered.Unix(), nowTS)

	redeliveredItem1, err := c.XREADGROUP(key, group, consumer1, XReadPending, 1)
	assert.NoError(t, err)
	assert.Equal(t, item1, redeliveredItem1)

	redeliveredItem2, err := c.XREADGROUP(key, group, consumer2, XReadPending, 1)
	assert.NoError(t, err)
	assert.Equal(t, item2, redeliveredItem2)

	pendingItems, err = c.XPENDING(key, group, 100)
	assert.NoError(t, err)
	assert.Equal(t, pendingItems[0].ID, item1[0].ID)
	assert.Equal(t, pendingItems[1].ID, item2[0].ID)
	assert.Equal(t, pendingItems[2].ID, item3[0].ID)
	assert.Equal(t, consumer1, pendingItems[0].Consumer)
	assert.Equal(t, consumer2, pendingItems[1].Consumer)
	assert.Equal(t, 3, len(pendingItems))
	assert.Equal(t, int64(2), pendingItems[0].DeliveryCount)
	assert.Equal(t, int64(2), pendingItems[1].DeliveryCount)
}
