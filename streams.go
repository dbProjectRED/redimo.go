package redimo

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type XID string

const (
	XStart  XID = "00000000000000000000-00000000000000000000"
	XEnd    XID = "99999999999999999999-99999999999999999999"
	XAutoID XID = "*"
)

func NewXID(ts time.Time, seq uint64) XID {
	timePart := fmt.Sprintf("%020d", ts.Unix())
	sequencePart := fmt.Sprintf("%020d", seq)

	return XID(strings.Join([]string{timePart, sequencePart}, "-"))
}

func (xid XID) String() string {
	return string(xid)
}

const sequenceSK = "_redimo/sequence"

func (xid XID) sequenceUpdateAction(key string, table string) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.condition(fmt.Sprintf("#%v < :%v", vk, vk), vk)
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{xid.String()}.toAV())

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: sequenceSK}.toAV(),
			TableName:                 aws.String(table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

func (xid XID) Next() XID {
	return NewXID(xid.Time(), xid.Seq()+1)
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

type StreamItem struct {
	ID     XID
	Fields map[string]string
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
	avm[pk] = StringValue{key}.toAV()
	avm[sk] = StringValue{i.ID.String()}.toAV()

	for k, v := range i.Fields {
		avm["_"+k] = StringValue{v}.toAV()
	}

	return avm
}

func (c Client) XACK(key string) (err error) { return }

func (c Client) XADD(key string, id XID, fields map[string]string) (returnedID XID, err error) {
	retry := true
	retryCount := 0

	for retry && retryCount < 2 {
		var actions []dynamodb.TransactWriteItem

		if id == XAutoID {
			now := time.Now()
			newSequence, err := c.HINCRBY(key, "_redimo/sequence/"+fmt.Sprintf("%020d", now.Unix()), big.NewInt(1))

			if err != nil {
				return id, err
			}

			id = NewXID(now, newSequence.Uint64())
		}

		actions = append(actions, StreamItem{ID: id, Fields: fields}.putAction(key, c.table))
		actions = append(actions, id.sequenceUpdateAction(key, c.table))

		_, err := c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err != nil {
			if conditionFailureError(err) && retryCount == 0 && c.xInit(key) == nil {
				// Steam may not have been initialized, but should be now
			} else {
				// err was an actual error
				return returnedID, err
			}
		} else {
			retry = false
		}
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
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{XStart.String()}.toAV())

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: sequenceSK}.toAV(),
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		},
	}
}

func (c Client) XCLAIM(key string) (err error) { return }

func (c Client) XDEL(key string, ids ...XID) (count int64, err error) {
	for _, id := range ids {
		resp, err := c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			Key:          keyDef{pk: key, sk: id.String()}.toAV(),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return count, err
		}

		if len(resp.Attributes) > 0 {
			count++
		}
	}

	return
}

func (c Client) XGROUP(key string) (err error) { return }

func (c Client) XINFO(key string) (err error) { return }

func (c Client) XLEN(key string, start, stop XID) (count int64, err error) {
	hasMoreResults := true

	var cursor map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", sk), sk)
		builder.values["start"] = dynamodb.AttributeValue{S: aws.String(start.String())}
		builder.values["stop"] = dynamodb.AttributeValue{S: aws.String(stop.String())}
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

func (c Client) XPENDING(key string) (err error) { return }

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
		builder.values["start"] = dynamodb.AttributeValue{S: aws.String(start.String())}
		builder.values["stop"] = dynamodb.AttributeValue{S: aws.String(stop.String())}
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
			fieldMap := make(map[string]string)

			for k, v := range resultItem {
				if strings.HasPrefix(k, "_") {
					fieldMap[k[1:]] = aws.StringValue(v.S)
				}
			}

			streamItems = append(streamItems, StreamItem{
				ID:     XID(aws.StringValue(resultItem[sk].S)),
				Fields: fieldMap,
			})
			count--
		}
	}

	return
}

func (c Client) XREAD(key string, from XID, count int64) (items []StreamItem, err error) {
	return c.XRANGE(key, from.Next(), XEnd, count)
}

func (c Client) XREADGROUP(key string) (err error) { return }

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
		builder.values["start"] = dynamodb.AttributeValue{S: aws.String(XStart.String())}
		builder.values["stop"] = dynamodb.AttributeValue{S: aws.String(XEnd.String())}
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

		var sortKeys []string

		for _, item := range resp.Items {
			if newCount == 0 {
				parsedItem := parseKey(item)
				sortKeys = append(sortKeys, parsedItem.sk)
			} else {
				newCount--
			}
		}

		if len(sortKeys) > 0 {
			deletedCount += int64(len(sortKeys))
			err = c.HDEL(key, sortKeys...)

			if err != nil {
				return deletedCount, err
			}
		}
	}

	return
}
