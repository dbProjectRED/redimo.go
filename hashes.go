package redimo

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c Client) HGET(key string, field string) (val ReturnValue, err error) {
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
					val:    ReturnValue{v.ToAV()},
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
		builder.updateSET(vk, v)

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

func (c Client) HMGET(key string, fields ...string) (values map[string]ReturnValue, err error) {
	values = make(map[string]ReturnValue)
	items := make([]dynamodb.TransactGetItem, len(fields))
	for i, field := range fields {
		items[i] = dynamodb.TransactGetItem{Get: &dynamodb.Get{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(),
			ProjectionExpression: aws.String(strings.Join([]string{sk, vk}, ", ")),
			TableName:            aws.String(c.table),
		}}
	}

	resp, err := c.ddbClient.TransactGetItemsRequest(&dynamodb.TransactGetItemsInput{
		TransactItems: items,
	}).Send(context.TODO())

	if err == nil {
		for _, r := range resp.Responses {
			pi := parseItem(r.Item)
			values[pi.sk] = pi.val
		}
	}

	return
}

func (c Client) HDEL(key string, fields ...string) (deletedCount int64, err error) {
	for _, field := range fields {
		resp, err := c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			Key: keyDef{
				pk: key,
				sk: field,
			}.toAV(),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return deletedCount, err
		}
		if len(resp.Attributes) > 0 {
			deletedCount++
		}
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

func (c Client) HGETALL(key string) (fieldValues map[string]ReturnValue, err error) {
	fieldValues = make(map[string]ReturnValue)
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})

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

func (c Client) HINCRBYFLOAT(key string, field string, delta float64) (after float64, err error) {
	rv, err := c.hIncr(key, field, FloatValue{delta})
	if err == nil {
		after = rv.Float()
	}

	return
}

func (c Client) hIncr(key string, field string, delta Value) (after ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":delta": delta.ToAV(),
		},
		Key:              keyDef{pk: key, sk: field}.toAV(),
		ReturnValues:     dynamodb.ReturnValueAllNew,
		TableName:        aws.String(c.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	}).Send(context.TODO())

	if err == nil {
		after = ReturnValue{resp.UpdateItemOutput.Attributes[vk]}
	}

	return
}

func (c Client) HINCRBY(key string, field string, delta int64) (after int64, err error) {
	rv, err := c.hIncr(key, field, IntValue{delta})

	if err == nil {
		after = rv.Int()
	}

	return
}

func (c Client) HKEYS(key string) (keys []string, err error) {
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.addConditionEquality(pk, StringValue{key})

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

func (c Client) HVALS(key string) (values []ReturnValue, err error) {
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
		builder.addConditionEquality(pk, StringValue{key})

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
	builder.updateSET(vk, value)
	builder.addConditionNotExists(pk)

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
