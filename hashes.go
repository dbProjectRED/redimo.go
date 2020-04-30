package redimo

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c Client) HGET(key string, field string) (val Value, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(),
		ProjectionExpression: aws.String(strings.Join([]string{vk}, ", ")),
		TableName:            aws.String(c.table),
	}).Send(context.TODO())
	if err == nil {
		val = parseItem(resp.Item).val
	}

	return
}

func (c Client) HSET(key string, fieldValues map[string]Value) (savedCount int64, err error) {
	items := make([]dynamodb.WriteRequest, 0, len(fieldValues))

	for field, v := range fieldValues {
		items = append(items, dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: itemDef{
					keyDef: keyDef{pk: key, sk: field},
					val:    v,
				}.eav(),
			},
		})
	}

	requestMap := map[string][]dynamodb.WriteRequest{}
	requestMap[c.table] = items

	for len(requestMap[c.table]) > 0 {
		attempting := len(requestMap[c.table])
		resp, err := c.ddbClient.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
			RequestItems: requestMap,
		}).Send(context.TODO())

		if err != nil {
			return savedCount, err
		}

		leftovers := len(resp.UnprocessedItems[c.table])
		savedCount = savedCount + int64(attempting) - int64(leftovers)
		requestMap = resp.UnprocessedItems
	}

	return
}

func (c Client) HMSET(key string, fieldValues map[string]Value) (err error) {
	items := make([]dynamodb.TransactWriteItem, 0, len(fieldValues))

	for field, v := range fieldValues {
		builder := newExpresionBuilder()
		builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, v.toAV())

		items = append(items, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       builder.conditionExpression(),
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				Key: keyDef{
					pk: key,
					sk: field,
				}.toAV(),
				TableName:        aws.String(c.table),
				UpdateExpression: builder.updateExpression(),
			},
		})
	}

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}).Send(context.TODO())

	return
}

func (c Client) HMGET(key string, fields ...string) (values []Value, err error) {
	items := make([]dynamodb.TransactGetItem, len(fields))
	for i, field := range fields {
		items[i] = dynamodb.TransactGetItem{Get: &dynamodb.Get{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(),
			ProjectionExpression: aws.String(vk),
			TableName:            aws.String(c.table),
		}}
	}

	resp, err := c.ddbClient.TransactGetItemsRequest(&dynamodb.TransactGetItemsInput{
		TransactItems: items,
	}).Send(context.TODO())

	if err == nil {
		for _, r := range resp.Responses {
			values = append(values, parseItem(r.Item).val)
		}
	}

	return
}

func (c Client) HDEL(key string, fields ...string) (err error) {
	deleteRequests := make([]dynamodb.WriteRequest, len(fields))
	for i, field := range fields {
		deleteRequests[i] = dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{Key: keyDef{
				pk: key,
				sk: field,
			}.toAV()},
		}
	}

	requestMap := map[string][]dynamodb.WriteRequest{
		c.table: deleteRequests,
	}

	for len(requestMap) > 0 {
		resp, err := c.ddbClient.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
			RequestItems: requestMap,
		}).Send(context.TODO())
		if err != nil {
			return err
		}

		requestMap = resp.UnprocessedItems
	}

	return
}

func (c Client) HEXISTS(key string, field string) (exists bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(),
		ProjectionExpression: aws.String(strings.Join([]string{pk}, ", ")),
		TableName:            aws.String(c.table),
	}).Send(context.TODO())
	if err == nil && len(resp.Item) > 0 {
		exists = true
	}

	return
}

func (c Client) HGETALL(key string) (fieldValues map[string]Value, err error) {
	fieldValues = make(map[string]Value)
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
		builder.values[pk] = dynamodb.AttributeValue{
			S: aws.String(key),
		}

		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return fieldValues, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item)
			fieldValues[parsedItem.sk] = parsedItem.val
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HINCRBYFLOAT(key string, field string, delta *big.Float) (after *big.Float, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":delta": NumericValue{delta}.toAV(),
		},
		Key:              keyDef{pk: key, sk: field}.toAV(),
		ReturnValues:     dynamodb.ReturnValueAllNew,
		TableName:        aws.String(c.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	}).Send(context.TODO())

	if err == nil {
		after, _ = parseItem(resp.UpdateItemOutput.Attributes).val.AsNumeric()
	}

	return
}

func (c Client) HINCRBY(key string, field string, delta *big.Int) (after *big.Int, err error) {
	afterFloat, err := c.HINCRBYFLOAT(key, field, new(big.Float).SetInt(delta))
	if err != nil {
		return
	}

	after, _ = afterFloat.Int(nil)

	return
}

func (c Client) HKEYS(key string) (keys []string, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
		builder.values[pk] = dynamodb.AttributeValue{
			S: aws.String(key),
		}
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
			ProjectionExpression:      aws.String(sk),
			Select:                    dynamodb.SelectSpecificAttributes,
		}).Send(context.TODO())

		if err != nil {
			return keys, err
		}

		for _, item := range resp.Items {
			parsedItem := parseItem(item)
			keys = append(keys, parsedItem.sk)
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HVALS(key string) (values []Value, err error) {
	all, err := c.HGETALL(key)
	if err == nil {
		for _, v := range all {
			values = append(values, v)
		}
	}

	return
}

func (c Client) HLEN(key string) (count int64, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
		builder.values[pk] = dynamodb.AttributeValue{
			S: aws.String(key),
		}
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
			Select:                    dynamodb.SelectCount,
		}).Send(context.TODO())

		if err != nil {
			return count, err
		}

		count += aws.Int64Value(resp.ScannedCount)

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) HSETNX(key string, field string, value Value) (ok bool, err error) {
	builder := newExpresionBuilder()
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, value.toAV())
	builder.condition(fmt.Sprintf("(attribute_not_exists(#%v))", pk), pk)

	_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		Key: keyDef{
			pk: key,
			sk: field,
		}.toAV(),
		TableName:        aws.String(c.table),
		UpdateExpression: builder.updateExpression(),
	}).Send(context.TODO())

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
