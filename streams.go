package redimo

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type StreamItemID string

const (
	StreamStart  StreamItemID = "00000000000000000000-00000000000000000000"
	StreamEnd    StreamItemID = "18446744073709551615-18446744073709551615"
	StreamAutoID StreamItemID = "*"
)

func NewStreamID(ts time.Time, seq int64) StreamItemID {
	timePart := fmt.Sprintf("%020d", ts.Unix())
	sequencePart := fmt.Sprintf("%020d", seq)

	return StreamItemID(strings.Join([]string{timePart, sequencePart}, "-"))
}

func (sid StreamItemID) String() string {
	return string(sid)
}

const sequenceSK = "_redimo/sequence"

func (sid StreamItemID) sequenceUpdateAction(key string, table string) dynamodb.TransactWriteItem {
	builder := newExpresionBuilder()
	builder.condition(fmt.Sprintf("#%v < :%v", vk, vk), vk)
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{sid.String()}.toAV())

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

type StreamItem struct {
	ID     StreamItemID
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

func (c Client) XADD(key string, id StreamItemID, fields map[string]string) (returnedID StreamItemID, err error) {
	retry := true
	retryCount := 0

	for retry && retryCount < 2 {
		var actions []dynamodb.TransactWriteItem

		if id == StreamAutoID {
			now := time.Now()
			newSequence, err := c.HINCRBY(key, "_redimo/sequence/"+fmt.Sprintf("%020d", now.Unix()), big.NewInt(1))

			if err != nil {
				return id, err
			}

			id = NewStreamID(now, newSequence.Int64())
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
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, StringValue{StreamStart.String()}.toAV())

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

func (c Client) XDEL(key string) (err error) { return }

func (c Client) XGROUP(key string) (err error) { return }

func (c Client) XINFO(key string) (err error) { return }

func (c Client) XLEN(key string) (err error) { return }

func (c Client) XPENDING(key string) (err error) { return }

func (c Client) XRANGE(key string, start, stop StreamItemID, count int64) (streamItems []StreamItem, err error) {
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
			Limit:                     aws.Int64(count),
			ScanIndexForward:          aws.Bool(true),
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
			streamItems = append(streamItems, StreamItem{
				ID:     StreamItemID(aws.StringValue(resultItem[sk].S)),
				Fields: nil,
			})
			count--
		}
	}

	return
}

func (c Client) XREAD(key string) (err error) { return }

func (c Client) XREADGROUP(key string) (err error) { return }

func (c Client) XREVRANGE(key string) (err error) { return }

func (c Client) XTRIM(key string) (err error) { return }
