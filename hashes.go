package redimo

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
)

func (c Client) HGET(key string, field string) (val Value, err error) {
	resp, err := c.client.GetItemRequest(&dynamodb.GetItemInput{
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
		resp, err := c.client.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
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
