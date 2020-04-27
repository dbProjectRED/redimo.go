package redimo

import (
	"context"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// GET conforms to https://redis.io/commands/get
func (c Client) GET(key string) (val Value, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            keyDef{pk: key, sk: defaultSK}.toAV(),
		TableName:      aws.String(c.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}
	val = parseItem(resp.Item).val
	return
}

// SET conforms to https://redis.io/commands/set
func (c Client) SET(key string, value Value, flags Flags) (ok bool, err error) {
	builder := newExpresionBuilder()

	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, value.toAV())

	if flags.has(IfNotExists) {
		builder.condition(fmt.Sprintf("attribute_not_exists(#%v)", pk), pk)
	}
	if flags.has(IfAlreadyExists) {
		builder.condition(fmt.Sprintf("attribute_exists(#%v)", pk), pk)
	}

	_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: defaultSK,
		}.toAV(),
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

// SETNX conforms to https://redis.io/commands/setnx
func (c Client) SETNX(key string, value Value) (ok bool, err error) {
	return c.SET(key, value, Flags{IfNotExists})
}

// GETSET https://redis.io/commands/getset
func (c Client) GETSET(key string, value Value) (oldValue Value, err error) {
	// TODO remove TTL, GETSET seems to require it
	builder := newExpresionBuilder()
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, value.toAV())
	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: defaultSK,
		}.toAV(),
		ReturnValues: dynamodb.ReturnValueAllOld,
		TableName:    aws.String(c.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Attributes) == 0 {
		return
	}
	oldValue = parseItem(resp.Attributes).val
	return
}

// MGET conforms to https://redis.io/commands/mget
func (c Client) MGET(keys ...string) (outputs []Value, err error) {
	inputRequests := make([]dynamodb.TransactGetItem, len(keys))
	outputs = make([]Value, len(keys))

	for i, key := range keys {
		inputRequests[i] = dynamodb.TransactGetItem{
			Get: &dynamodb.Get{
				Key: keyDef{
					pk: key,
					sk: defaultSK,
				}.toAV(),
				ProjectionExpression: aws.String(vk),
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
	for i, out := range resp.Responses {
		if len(out.Item) > 0 {
			outputs[i] = parseItem(out.Item).val
		} else {
			outputs[i] = nil
		}
	}
	return
}

// MSET conforms to https://redis.io/commands/mset
func (c Client) MSET(data map[string]Value) (err error) {
	_, err = c._mset(data, Flags{})
	return
}

// MSETNX conforms to https://redis.io/commands/msetnx
func (c Client) MSETNX(data map[string]Value) (ok bool, err error) {
	return c._mset(data, Flags{IfNotExists})
}

func (c Client) _mset(data map[string]Value, flags Flags) (ok bool, err error) {
	var inputs []dynamodb.TransactWriteItem

	for k, v := range data {
		builder := newExpresionBuilder()

		if flags.has(IfNotExists) {
			builder.condition(fmt.Sprintf("(attribute_not_exists(#%v))", pk), pk)
		}

		builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, v.toAV())
		inputs = append(inputs, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       builder.conditionExpression(),
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				Key: keyDef{
					pk: k,
					sk: defaultSK,
				}.toAV(),
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

// INCRBYFLOAT https://redis.io/commands/incrbyfloat
func (c Client) INCRBYFLOAT(key string, delta *big.Float) (after *big.Float, err error) {
	builder := newExpresionBuilder()
	builder.keys[vk] = struct{}{}
	resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: builder.expressionAttributeNames(),
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":delta": NumericValue{delta}.toAV(),
		},
		Key:              keyDef{pk: key, sk: defaultSK}.toAV(),
		ReturnValues:     dynamodb.ReturnValueAllNew,
		TableName:        aws.String(c.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	}).Send(context.TODO())
	if err == nil {
		after, _ = parseItem(resp.UpdateItemOutput.Attributes).val.AsNumeric()
	}
	return
}

func (c Client) _incrByInt(key string, delta *big.Int) (after *big.Int, err error) {
	floatAfter, err := c.INCRBYFLOAT(key, new(big.Float).SetInt(delta))
	if err == nil {
		after, _ = floatAfter.Int(nil)
	}
	return
}

// INCR https://redis.io/commands/incr
func (c Client) INCR(key string) (after *big.Int, err error) {
	return c._incrByInt(key, big.NewInt(1))
}

// DECR https://redis.io/commands/decr
func (c Client) DECR(key string) (after *big.Int, err error) {
	return c._incrByInt(key, big.NewInt(-1))
}

// INRCBY https://redis.io/commands/incrby
func (c Client) INCRBY(key string, delta *big.Int) (after *big.Int, err error) {
	return c._incrByInt(key, delta)
}

// DECRBY https://redis.io/commands/decrby
func (c Client) DECRBY(key string, delta *big.Int) (after *big.Int, err error) {
	return c._incrByInt(key, new(big.Int).Neg(delta))
}
