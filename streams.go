package redimo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// XID holds a stream item ID, and consists of a timestamp (one second resolution)
// and a sequence number.
//
// Most code will not need to generate XIDs – using XAutoID with XADD is the most common usage.
// But if you do need to generate XIDs for insertion with XADD, the NewXID methods creates a complete XID.
//
// To generate time based XIDs for time range queries with XRANGE or XREVRANGE, use
// NewTimeXID(startTime).First() and NewTimeXID(endTime).Last(). Calling Last() is especially important
// because without it none of the items in the last second of the range will match – you need
// the last possible sequence number in the last second of the range, which is what the Last() method provides.
type XID string

var ErrXGroupNotInitialized = errors.New("consumer group not initialized with XGROUP")

const consumerKey = "cnk"
const lastDeliveryTimestampKey = "ldk"
const deliveryCountKey = "dck"

const XStart XID = "00000000000000000000-00000000000000000000"
const XEnd XID = "99999999999999999999-99999999999999999999"
const XAutoID XID = "*"

// NewXID creates an XID with the given timestamp and sequence number.
func NewXID(ts time.Time, seq uint64) XID {
	timePart := fmt.Sprintf("%020d", ts.Unix())
	sequencePart := fmt.Sprintf("%020d", seq)

	return XID(strings.Join([]string{timePart, sequencePart}, "-"))
}

// NewTimeXID creates an XID with the given timestamp. To get the first or the last
// XID in this timestamp, use the First() or the Last() methods. This is especially
// important when using constructed XIDs inside a range call like XRANGE or XREVRANGE.
func NewTimeXID(ts time.Time) XID {
	return NewXID(ts, 0)
}

func (xid XID) String() string {
	return string(xid)
}

func xSequenceKey(key string) keyDef {
	return keyDef{
		pk: strings.Join([]string{"_redimo", "seq", key}, "/"),
		sk: "seq",
	}
}

func (xid XID) sequenceUpdateAction(key string, table string) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.condition(fmt.Sprintf("#%v < :%v", vk, vk), vk)
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{xid.String()}.ToAV())

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       xSequenceKey(key).toAV(),
			TableName:                 aws.String(table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

// Next returns the next valid XID at the same time – it simply returns a new XID with the next sequence number.
func (xid XID) Next() XID {
	return NewXID(xid.Time(), xid.Seq()+1)
}

// Prev returns the previous valid XID at the same time – it simply returns a new XID with the previous sequence number.
func (xid XID) Prev() XID {
	if xid.Seq() <= 1 {
		return xid
	}

	return NewXID(xid.Time(), xid.Seq()-1)
}

func (xid XID) Time() time.Time {
	parts := strings.Split(xid.String(), "-")
	tsec, _ := strconv.ParseInt(parts[0], 10, 64)

	return time.Unix(tsec, 0)
}

func (xid XID) Seq() uint64 {
	parts := strings.Split(xid.String(), "-")
	seq, _ := strconv.ParseUint(parts[1], 10, 64)

	return seq
}

func (xid XID) av() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{S: aws.String(xid.String())}
}

// First returns the first valid XID at this timestamp. Useful for the start parameter of XRANGE or XREVRANGE.
func (xid XID) First() XID {
	timePart := fmt.Sprintf("%020d", xid.Time().Unix())
	return XID(strings.Join([]string{timePart, "00000000000000000000"}, "-"))
}

// Last returns the last valid XID at this timestamp. Useful for the end parameter of XRANGE or XREVRANGE.
// Note that if the XID used as an end in the range simply based on the timestamp, the sequence number will be zero,
// so the query will exclude all the items in end second. This will effectively transform the query to '< endTime'
// instead of '<= endTime'. Using Last() prevents this mistake, if that is your intention.
func (xid XID) Last() XID {
	timePart := fmt.Sprintf("%020d", xid.Time().Unix())
	return XID(strings.Join([]string{timePart, "99999999999999999999"}, "-"))
}

type StreamItem struct {
	ID     XID
	Fields map[string]ReturnValue
}

type PendingItem struct {
	ID            XID
	Consumer      string
	LastDelivered time.Time
	DeliveryCount int64
}

func (pi PendingItem) toPutAction(key string, table string) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.updateSET(consumerKey, StringValue{pi.Consumer})
	builder.updateSET(lastDeliveryTimestampKey, IntValue{pi.LastDelivered.Unix()})
	builder.clauses["ADD"] = append(builder.clauses["ADD"], fmt.Sprintf("#%v :delta", deliveryCountKey))
	builder.keys[deliveryCountKey] = struct{}{}
	builder.values["delta"] = IntValue{1}.ToAV()

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: pi.ID.String()}.toAV(),
			TableName:                 aws.String(table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

func (pi PendingItem) updateDeliveryAction(key string, table string) *dynamodb.UpdateItemInput {
	builder := newExpresionBuilder()
	builder.addConditionEquality(consumerKey, StringValue{pi.Consumer})
	builder.updateSET(lastDeliveryTimestampKey, IntValue{time.Now().Unix()})
	builder.clauses["ADD"] = append(builder.clauses["ADD"], fmt.Sprintf("#%v :delta", deliveryCountKey))
	builder.keys[deliveryCountKey] = struct{}{}
	builder.values["delta"] = IntValue{1}.ToAV()

	return &dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key:                       keyDef{pk: key, sk: pi.ID.String()}.toAV(),
		TableName:                 aws.String(table),
		UpdateExpression:          builder.updateExpression(),
	}
}

func parsePendingItem(avm map[string]dynamodb.AttributeValue) (pi PendingItem) {
	pi.ID = XID(aws.StringValue(avm[sk].S))
	pi.Consumer = aws.StringValue(avm[consumerKey].S)
	timestamp, _ := strconv.ParseInt(aws.StringValue(avm[lastDeliveryTimestampKey].N), 10, 64)
	pi.LastDelivered = time.Unix(timestamp, 0)
	deliveryCount, _ := strconv.ParseInt(aws.StringValue(avm[deliveryCountKey].N), 10, 64)
	pi.DeliveryCount = deliveryCount

	return
}

func (i StreamItem) putAction(key string, table string) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      i.toAV(key),
			TableName: aws.String(table),
		},
	}
}

func (i StreamItem) toAV(key string) map[string]dynamodb.AttributeValue {
	avm := make(map[string]dynamodb.AttributeValue)
	avm[pk] = StringValue{key}.ToAV()
	avm[sk] = StringValue{i.ID.String()}.ToAV()

	for k, v := range i.Fields {
		avm["_"+k] = v.ToAV()
	}

	return avm
}

func (c Client) XACK(key string, group string, ids ...XID) (acknowledgedIds []XID, err error) {
	for _, id := range ids {
		resp, err := c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			Key:          keyDef{pk: c.xGroupKey(key, group), sk: id.String()}.toAV(),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return acknowledgedIds, err
		}

		if len(resp.Attributes) > 0 {
			acknowledgedIds = append(acknowledgedIds, id)
		}
	}

	return
}

// XADD adds the given fields as a item on the stream at key. If the stream does not exist,
// it will be initialized.
//
// If the XID passed in is XAutoID, an ID will be automatically generated on the current time
// and a sequence generator.
//
// Note that if you pass in your own ID, the stream will never allow you to insert an item with
// an ID less than the greatest ID present in the stream – the stream can only move forwards. This
// guarantees that if you've read entries up to a given XID using XREAD, you can always continue
// reading from that last XID without fear of missing anything, because the IDs are always increasing.
//
// Works similar to https://redis.io/commands/xadd
func (c Client) XADD(key string, id XID, fields map[string]Value) (returnedID XID, err error) {
	retry := true
	retryCount := 0

	for retry && retryCount < 2 {
		var actions []dynamodb.TransactWriteItem

		if id == XAutoID {
			now := time.Now()
			newSequence, err := c.INCR(strings.Join([]string{"_redimo", "xcount", key}, "/"))

			if err != nil {
				return id, err
			}

			id = NewXID(now, uint64(newSequence))
		}

		wrappedFields := make(map[string]ReturnValue)

		for k, v := range fields {
			wrappedFields[k] = ReturnValue{v.ToAV()}
		}

		actions = append(actions, StreamItem{ID: id, Fields: wrappedFields}.putAction(key, c.table))
		actions = append(actions, id.sequenceUpdateAction(key, c.table))

		_, err := c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err != nil {
			if conditionFailureError(err) && retryCount == 0 {
				// Steam may not have been initialized, let's try initializing
				err = c.xInit(key)
				if err != nil {
					return returnedID, err
				}
			} else {
				return returnedID, err
			}
		} else {
			retry = false
		}
		// Likely happened because of contention, let's retry once.
		retryCount++
	}

	return id, err
}

func (c Client) xInit(key string) (err error) {
	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodb.TransactWriteItem{c.xInitAction(key)},
	}).Send(context.TODO())
	if conditionFailureError(err) {
		err = nil
	}

	return
}

func (c Client) xInitAction(key string) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.addConditionNotExists(vk)
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{XStart.String()}.ToAV())

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       xSequenceKey(key).toAV(),
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

func (c Client) XCLAIM(key string, group string, consumer string, lastDeliveredBefore time.Time, ids ...XID) (items []StreamItem, err error) {
	for _, id := range ids {
		builder := newExpresionBuilder()
		builder.addConditionExists(pk)
		builder.addConditionLessThanOrEqualTo(lastDeliveryTimestampKey, IntValue{lastDeliveredBefore.Unix()})
		builder.updateSET(lastDeliveryTimestampKey, IntValue{time.Now().Unix()})
		builder.updateSET(deliveryCountKey, IntValue{0})
		builder.updateSET(consumerKey, StringValue{consumer})

		_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: c.xGroupKey(key, group), sk: id.String()}.toAV(),
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		}).Send(context.TODO())

		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return items, err
		}

		fetchedItems, err := c.XRANGE(key, id, id, 1)

		if err != nil || len(fetchedItems) < 1 {
			return items, fmt.Errorf("could not loat stream item: %w", err)
		}

		items = append(items, fetchedItems[0])
	}

	return items, nil
}

// XDEL removes the given IDs and returns the IDs that were actually deleted as part of this operation.
//
// Note that this operation is not atomic across given IDs – it's possible that an error is returned
// based on a problem deleting one of the IDs when the others have been deleted. Even when an error is returned,
// the items that were deleted will still be populated.
//
// Works similar to https://redis.io/commands/xdel
func (c Client) XDEL(key string, ids ...XID) (deletedItems []XID, err error) {
	for _, id := range ids {
		resp, err := c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			Key:          keyDef{pk: key, sk: id.String()}.toAV(),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return deletedItems, err
		}

		if len(resp.Attributes) > 0 {
			deletedItems = append(deletedItems, id)
		}
	}

	return
}

func (c Client) XGROUP(key string, group string, start XID) (err error) {
	err = c.xGroupCursorSet(key, group, start)
	return
}

func (c Client) xGroupCursorSet(key string, group string, start XID) error {
	cursorKey := c.xGroupCursorKey(key, group)
	_, err := c.HSET(cursorKey.pk, map[string]Value{cursorKey.sk: StringValue{start.String()}})

	return err
}

func (c Client) xGroupCursorGet(key string, group string) (id XID, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key:            c.xGroupCursorKey(key, group).toAV(),
		TableName:      aws.String(c.table),
	}).Send(context.TODO())
	if err != nil {
		return
	}

	cursor := aws.StringValue(resp.Item[vk].S)
	if cursor == "" {
		return id, ErrXGroupNotInitialized
	}

	return XID(cursor), nil
}

func (c Client) xGroupCursorKey(key string, group string) keyDef {
	return keyDef{pk: c.xGroupKey(key, group), sk: "_redimo/cursor"}
}

func (c Client) xGroupKey(key string, group string) string {
	return strings.Join([]string{"_redimo", key, group}, "/")
}

func (c Client) XLEN(key string, start, stop XID) (count int64, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		builder.values["start"] = start.av()
		builder.values["stop"] = stop.av()
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         cursor,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			ScanIndexForward:          aws.Bool(true),
			Select:                    dynamodb.SelectCount,
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return count, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			cursor = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		count += aws.Int64Value(resp.Count)
	}

	return
}

func (c Client) XPENDING(key string, group string, count int64) (pendingItems []PendingItem, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults && count > 0 {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{c.xGroupKey(key, group)})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		builder.values["start"] = XStart.av()
		builder.values["stop"] = XEnd.av()

		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         cursor,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     aws.Int64(count),
			ScanIndexForward:          aws.Bool(true),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return pendingItems, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			cursor = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		for _, item := range resp.Items {
			pendingItems = append(pendingItems, parsePendingItem(item))
			count--
		}
	}

	return
}

// XRANGE fetches the stream records between two XIDs, inclusive of both the start and end IDs, limited to the count.
//
// If you receive the entire count you've asked for, it's reasonable to suppose there might be more items in the given
// range that were not returned because they would exceed the count – in this case you can call the XID.Next() method
// on the last received stream ID for an XID to use as the start of the next call.
//
// Common uses include fetching a single item based on XID, which would be
//     XRANGE(key, id, id, 1)
// or fetching records in the month of February, like
//     XRANGE(key, NewTimeXID(beginningOfFebruary).First(), NewTimeXID(endOfFebruary).Last(), 1000)
//     XRANGE(key, NewTimeXID(beginningOfFebruary).First(), NewTimeXID(beginningOfMarch).First(), 1000)
// Note that the two calls are equivalent, because this operation uses the DynamoDB BETWEEN operator, which translates to
//     start <= id <= end
// There is are no offset or pagination parameters required, because when the full count is hit the next page of items can
// be fetched as follows:
//     XRANGE(key, lastFetchedItemID.Next(), NewTimeXID(endOfFebruary).Last(), 1000)
// See the XID docs for more information on how to generate start and stop XIDs based on time.
//
// Works similar to https://redis.io/commands/xrange
func (c Client) XRANGE(key string, start, stop XID, count int64) (streamItems []StreamItem, err error) {
	return c.xRange(key, start, stop, count, true)
}

func (c Client) xRange(key string, start, stop XID, count int64, forward bool) (streamItems []StreamItem, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults && count > 0 {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		builder.values["start"] = start.av()
		builder.values["stop"] = stop.av()
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         cursor,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     aws.Int64(count),
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return streamItems, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			cursor = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		for _, resultItem := range resp.Items {
			streamItems = append(streamItems, parseStreamItem(resultItem))
			count--
		}
	}

	return
}

func parseStreamItem(item map[string]dynamodb.AttributeValue) (si StreamItem) {
	si.Fields = make(map[string]ReturnValue)

	for k, v := range item {
		if strings.HasPrefix(k, "_") {
			si.Fields[k[1:]] = ReturnValue{v}
		}
	}

	si.ID = XID(aws.StringValue(item[sk].S))

	return
}

func (c Client) xGroupCursorPushAction(key string, group string, id XID) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.updateSET(vk, StringValue{id.String()})
	builder.addConditionLessThan(vk, StringValue{id.String()})

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       c.xGroupCursorKey(key, group).toAV(),
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

// XREAD reads items sequentially from a stream. The structure of a stream guarantees that the XIDs are
// always increasing. This implies that calling XREAD in a loop and passing in the XID of the last item read
// will allow iteration over all items reliably.
//
// To start reading a stream from the beginning, use the special XStart XID.
//
// Works similar to https://redis.io/commands/xread
func (c Client) XREAD(key string, from XID, count int64) (items []StreamItem, err error) {
	return c.XRANGE(key, from.Next(), XEnd, count)
}

type XReadOption string

const (
	XReadPending    XReadOption = "PENDING"
	XReadNew        XReadOption = "READ_NEW"
	XReadNewAutoACK XReadOption = "READ_NEW_NO_ACK"
)

func (c Client) xGroupReadPending(key string, group string, consumer string, count int64) (items []StreamItem, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults && count > 0 {
		query := newExpresionBuilder()
		query.addConditionEquality(pk, StringValue{c.xGroupKey(key, group)})
		query.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		query.values["start"] = StringValue{XStart.String()}.ToAV()
		query.values["stop"] = StringValue{XEnd.String()}.ToAV()
		query.values[consumerKey] = StringValue{consumer}.ToAV()
		query.keys[consumerKey] = struct{}{}
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         cursor,
			ExpressionAttributeNames:  query.expressionAttributeNames(),
			ExpressionAttributeValues: query.expressionAttributeValues(),
			FilterExpression:          aws.String(fmt.Sprintf("#%v = :%v", consumerKey, consumerKey)),
			KeyConditionExpression:    query.conditionExpression(),
			Limit:                     aws.Int64(count),
			ScanIndexForward:          aws.Bool(true),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return items, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			cursor = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		for _, item := range resp.Items {
			pendingItem := parsePendingItem(item)

			_, err = c.ddbClient.UpdateItemRequest(pendingItem.updateDeliveryAction(c.xGroupKey(key, group), c.table)).Send(context.TODO())
			if err != nil {
				return items, err
			}

			fetchedItems, err := c.XRANGE(key, pendingItem.ID, pendingItem.ID, 1)
			if err != nil || len(fetchedItems) < 1 {
				return items, err
			}

			items = append(items, fetchedItems[0])
			count--
		}
	}

	return
}

func (c Client) XREADGROUP(key string, group string, consumer string, option XReadOption, maxCount int64) (items []StreamItem, err error) {
	if option == XReadPending {
		return c.xGroupReadPending(key, group, consumer, maxCount)
	}

	retryCount := 0

	for retryCount < 5 {
		currentCursor, err := c.xGroupCursorGet(key, group)
		if err != nil {
			return items, err
		}

		items, err := c.XRANGE(key, currentCursor.Next(), XEnd, 1)

		if err != nil || len(items) == 0 {
			return items, err
		}

		item := items[0]

		var actions []dynamodb.TransactWriteItem
		actions = append(actions, c.xGroupCursorPushAction(key, group, item.ID))

		if option == XReadNew {
			actions = append(actions, PendingItem{
				ID:            item.ID,
				Consumer:      consumer,
				LastDelivered: time.Now(),
			}.toPutAction(c.xGroupKey(key, group), c.table))
		}

		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err == nil {
			return items, nil
		}

		if !conditionFailureError(err) {
			return items, err
		}
		retryCount++
	}

	return items, errors.New("too much contention")
}

// XREVRANGE is similar to XRANGE, but in reverse order. The stream items in descending chronological order. Using the
// same example as XRANGE, when fetching items in reverse order there are some differences when paginating. The first
// set of records can be fetched using:
//     XRANGE(key, NewTimeXID(endOfFebruary).Last(), NewTimeXID(beginningOfFebruary).First(), 1000)
// he next page can be fetched using
//     XRANGE(key, lastFetchedItemID.Prev(), NewTimeXID(beginningOfFebruary).First(), 1000)
// Works similar to https://redis.io/commands/xrevrange
func (c Client) XREVRANGE(key string, end, start XID, count int64) (streamItems []StreamItem, err error) {
	return c.xRange(key, start, end, count, false)
}

func (c Client) XTRIM(key string, newCount int64) (deletedCount int64, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		builder.values["start"] = XStart.av()
		builder.values["stop"] = XEnd.av()
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         cursor,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			ProjectionExpression:      aws.String(strings.Join([]string{pk, sk}, ",")),
			ScanIndexForward:          aws.Bool(false),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return deletedCount, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			cursor = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		var idsToDelete []XID

		for _, item := range resp.Items {
			if newCount == 0 {
				parsedItem := parseKey(item)
				idsToDelete = append(idsToDelete, XID(parsedItem.sk))
			} else {
				newCount--
			}
		}

		if len(idsToDelete) > 0 {
			deletedCount += int64(len(idsToDelete))
			_, err = c.XDEL(key, idsToDelete...)

			if err != nil {
				return deletedCount, err
			}
		}
	}

	return
}
