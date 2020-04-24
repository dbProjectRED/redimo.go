package redimo

import (
	"context"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// GET conforms to https://redis.io/commands/get
func (rc Client) GET(key string) (val Value, err error) {
	resp, err := rc.client.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(rc.strongConsistency),
		Key:            keyDef{pk: key, sk: defaultSK}.toAV(),
		TableName:      aws.String(rc.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}
	val = parseItem(resp.Item).val
	return
}

// SET conforms to https://redis.io/commands/set
func (rc Client) SET(key string, value Value, flags Flags) (ok bool, err error) {
	builder := newExpresionBuilder()

	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, value.toAV())

	if flags.has(IfNotExists) {
		builder.condition(fmt.Sprintf("attribute_not_exists(#%v)", pk), pk)
	}
	if flags.has(IfAlreadyExists) {
		builder.condition(fmt.Sprintf("attribute_exists(#%v)", pk), pk)
	}

	_, err = rc.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: defaultSK,
		}.toAV(),
		TableName: aws.String(rc.table),
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
func (rc Client) SETNX(key string, value Value) (ok bool, err error) {
	return rc.SET(key, value, Flags{IfNotExists})
}

// SETEX conforms to https://redis.io/commands/setex
func (rc Client) SETEX(key string, value Value) (err error) {
	_, err = rc.SET(key, value, Flags{})
	return
}

// GETSET https://redis.io/commands/getset
func (rc Client) GETSET(key string, value Value) (oldValue Value, err error) {
	// TODO remove TTL, GETSET seems to require it
	builder := newExpresionBuilder()
	builder.SET(fmt.Sprintf("#%v = :%v", vk, vk), vk, value.toAV())
	resp, err := rc.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       builder.conditionExpression(),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		UpdateExpression:          builder.updateExpression(),
		Key: keyDef{
			pk: key,
			sk: defaultSK,
		}.toAV(),
		ReturnValues: dynamodb.ReturnValueAllOld,
		TableName:    aws.String(rc.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Attributes) == 0 {
		return
	}
	oldValue = parseItem(resp.Attributes).val
	return
}

// MGET conforms to https://redis.io/commands/mget
func (rc Client) MGET(keys []string) (outputs []Value, err error) {
	inputRequests := make([]dynamodb.TransactGetItem, len(keys))
	outputs = make([]Value, len(keys))

	for _, key := range keys {
		inputRequests = append(inputRequests, dynamodb.TransactGetItem{
			Get: &dynamodb.Get{
				ExpressionAttributeNames: expressionAttributeNames,
				Key: keyDef{
					pk: key,
					sk: "0",
				}.toAV(),
				ProjectionExpression: aws.String("#val"),
				TableName:            aws.String(rc.table),
			},
		})
	}
	resp, err := rc.client.TransactGetItemsRequest(&dynamodb.TransactGetItemsInput{
		TransactItems: inputRequests,
	}).Send(context.TODO())
	if err != nil {
		return
	}
	for _, out := range resp.Responses {
		if len(out.Item) > 0 {
			outputs = append(outputs, parseItem(out.Item).val)
		} else {
			outputs = append(outputs, nil)
		}
	}
	return
}

// MSET conforms to https://redis.io/commands/mset
func (rc Client) MSET(data map[string]Value) (err error) {
	_, err = rc._mset(data, Flags{})
	return
}

// MSETNX conforms to https://redis.io/commands/msetnx
func (rc Client) MSETNX(data map[string]Value) (ok bool, err error) {
	return rc._mset(data, Flags{IfNotExists})
}

func (rc Client) _mset(data map[string]Value, flags Flags) (ok bool, err error) {
	var inputs []dynamodb.TransactWriteItem
	var condition *string
	if flags.has(IfNotExists) {
		condition = aws.String("(attribute_not_exists(#pk))")
	}
	for k, v := range data {
		inputs = append(inputs, dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				ConditionExpression:      condition,
				ExpressionAttributeNames: expressionAttributeNames,
				Item: itemDef{
					keyDef: keyDef{
						pk: k,
						sk: "0",
					},
					val: v,
				}.toAV(),
				TableName: aws.String(rc.table),
			},
		})
	}
	_, err = rc.client.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		ClientRequestToken: nil,
		TransactItems:      inputs,
	}).Send(context.TODO())
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException,
				dynamodb.ErrCodeTransactionInProgressException,
				dynamodb.ErrCodeTransactionConflictException,
				dynamodb.ErrCodeTransactionCanceledException:
				return false, nil
			}
		}
		return
	}
	return true, nil
}

// INCRBYFLOAT https://redis.io/commands/incrbyfloat
func (rc Client) INCRBYFLOAT(key string, delta *big.Float) (after *big.Float, err error) {
	resp, err := rc.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: expressionAttributeNames,
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":delta": NumericValue{delta}.toAV(),
		},
		Key:              keyDef{pk: key, sk: "0"}.toAV(),
		ReturnValues:     dynamodb.ReturnValueAllNew,
		TableName:        aws.String(rc.table),
		UpdateExpression: aws.String("ADD #val :delta"),
	}).Send(context.TODO())
	if err == nil {
		after, _ = parseItem(resp.UpdateItemOutput.Attributes).val.AsNumeric()
	}
	return
}

// INCR https://redis.io/commands/incr
func (rc Client) INCR(key string) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, big.NewFloat(1.0))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// DECR https://redis.io/commands/decr
func (rc Client) DECR(key string) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, big.NewFloat(-1.0))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// INRCBY https://redis.io/commands/incrby
func (rc Client) INCRBY(key string, delta *big.Int) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, new(big.Float).SetInt(delta))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// DECRBY https://redis.io/commands/decrby
func (rc Client) DECRBY(key string, delta *big.Int) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, new(big.Float).SetInt(new(big.Int).Neg(delta)))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}