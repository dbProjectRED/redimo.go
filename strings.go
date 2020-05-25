package redimo

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const emptySK = "/"

// GET fetches the value at the given key. If the key does not exist, the ReturnValue will be Empty().
//
// Works similar to https://redis.io/commands/get
func (c Client) GET(key string) (val ReturnValue, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            keyDef{pk: key, sk: emptySK}.toAV(c),
		TableName:      aws.String(c.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}

	val = ReturnValue{resp.Item[vk]}

	return
}

// SET stores the given Value at the given key. If called as SET("key", "value", None), SET is
// unconditional and is not expected to fail.
//
// The condition flags IfNotExists and IfAlreadyExists can be specified, and if they are
// the SET becomes conditional and will return false if the condition fails.
//
// Works similar to https://redis.io/commands/set
func (c Client) SET(key string, value Value, flag Flag) (ok bool, err error) {
	builder := newExpresionBuilder()

	builder.updateSET(vk, value)

	if flag == IfNotExists {
		builder.addConditionNotExists(c.pk)
	}

	if flag == IfAlreadyExists {
		builder.addConditionExists(c.pk)
	}

	_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: emptySK,
		}.toAV(c),
		TableName: aws.String(c.table),
	}).Send(context.TODO())
	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return
	}

	return true, nil
}

// SETNX is equivalent to SET(key, value, Flags{IfNotExists})
//
// Works similar to https://redis.io/commands/setnx
func (c Client) SETNX(key string, value Value) (ok bool, err error) {
	return c.SET(key, value, IfNotExists)
}

// GETSET gets the value at the key and atomically sets it to a new value.
//
// Works similar to https://redis.io/commands/getset
func (c Client) GETSET(key string, value Value) (oldValue ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.updateSET(vk, value)

	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: emptySK,
		}.toAV(c),
		ReturnValues: dynamodb.ReturnValueAllOld,
		TableName:    aws.String(c.table),
	}).Send(context.TODO())

	if err != nil || len(resp.Attributes) == 0 {
		return
	}

	oldValue = parseItem(resp.Attributes, c).val

	return
}

// MGET fetches the given keys atomically in a transaction. The call is limited to 25 keys and 4MB.
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
//
// Works similar to https://redis.io/commands/mget
func (c Client) MGET(keys ...string) (values map[string]ReturnValue, err error) {
	values = make(map[string]ReturnValue)
	inputRequests := make([]dynamodb.TransactGetItem, len(keys))

	for i, key := range keys {
		inputRequests[i] = dynamodb.TransactGetItem{
			Get: &dynamodb.Get{
				Key: keyDef{
					pk: key,
					sk: emptySK,
				}.toAV(c),
				ProjectionExpression: aws.String(strings.Join([]string{vk, c.pk}, ", ")),
				TableName:            aws.String(c.table),
			},
		}
	}

	resp, err := c.ddbClient.TransactGetItemsRequest(&dynamodb.TransactGetItemsInput{
		TransactItems: inputRequests,
	}).Send(context.TODO())

	if err != nil {
		return
	}

	for _, item := range resp.Responses {
		pi := parseItem(item.Item, c)
		values[pi.pk] = pi.val
	}

	return
}

// MSET sets the given keys and values atomically in a transaction. The call is limited to 25 keys and 4MB.
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
//
// Works similar to https://redis.io/commands/mset
func (c Client) MSET(data map[string]Value) (err error) {
	_, err = c.mset(data, Flags{})
	return
}

// MSETNX sets the given keys and values atomically in a transaction, but only if none of the given
// keys exist. If one or more of the keys already exist, nothing will be changed and MSETNX will return false.
//
// Works similar to https://redis.io/commands/msetnx
func (c Client) MSETNX(data map[string]Value) (ok bool, err error) {
	return c.mset(data, Flags{IfNotExists})
}

func (c Client) mset(data map[string]Value, flags Flags) (ok bool, err error) {
	inputs := make([]dynamodb.TransactWriteItem, 0, len(data))

	for k, v := range data {
		builder := newExpresionBuilder()

		if flags.has(IfNotExists) {
			builder.addConditionNotExists(c.pk)
		}

		builder.updateSET(vk, v)

		inputs = append(inputs, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       builder.conditionExpression(),
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				Key: keyDef{
					pk: k,
					sk: emptySK,
				}.toAV(c),
				TableName:        aws.String(c.table),
				UpdateExpression: builder.updateExpression(),
			},
		})
	}

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		ClientRequestToken: nil,
		TransactItems:      inputs,
	}).Send(context.TODO())

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c Client) INCRBYFLOAT(key string, delta float64) (after float64, err error) {
	rv, err := c.incr(key, FloatValue{delta})
	if err == nil {
		after = rv.Float()
	}

	return
}

func (c Client) incr(key string, value Value) (newValue ReturnValue, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":delta": value.ToAV(),
		},
		Key:              keyDef{pk: key, sk: emptySK}.toAV(c),
		ReturnValues:     dynamodb.ReturnValueAllNew,
		TableName:        aws.String(c.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	}).Send(context.TODO())

	if err == nil {
		newValue = ReturnValue{resp.UpdateItemOutput.Attributes[vk]}
	}

	return
}

func (c Client) INCR(key string) (after int64, err error) {
	return c.INCRBY(key, 1)
}

func (c Client) DECR(key string) (after int64, err error) {
	return c.INCRBY(key, -1)
}

func (c Client) INCRBY(key string, delta int64) (after int64, err error) {
	rv, err := c.incr(key, IntValue{delta})
	if err == nil {
		after = rv.Int()
	}

	return
}

func (c Client) DECRBY(key string, delta int64) (after int64, err error) {
	return c.INCRBY(key, -delta)
}
