package redimo

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
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
	var items []dynamodb.WriteRequest
	for field, v := range fieldValues {
		builder := newExpresionBuilder()
		builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, v.toAV())
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
	var items []dynamodb.TransactWriteItem
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
	var items []dynamodb.TransactGetItem
	for _, field := range fields {
		items = append(items, dynamodb.TransactGetItem{Get: &dynamodb.Get{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(),
			ProjectionExpression: aws.String(vk),
			TableName:            aws.String(c.table),
		}})
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
