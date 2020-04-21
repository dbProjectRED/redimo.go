package redimo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"math/big"
	"strings"
)

// GET conforms to https://redis.io/commands/get
func (rc RedimoClient) GET(key string) (val Value, ok bool, err error) {
	resp, err := rc.client.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(rc.strongConsistency),
		Key:            keyDef{pk: key, sk: "0"}.toAV(),
		TableName:      aws.String(rc.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}
	val = parseItem(resp.Item).val
	return
}

// SET conforms to https://redis.io/commands/set
func (rc RedimoClient) SET(key string, value Value, ttl int64, flags Flags) (ok bool, err error) {
	item := itemDef{
		keyDef: keyDef{
			pk: key,
			sk: "0",
		},
		val: value,
		ttl: ttl,
	}
	var conditionExpressions []string
	var setClauses []string

	setClauses = append(setClauses, "#val = :val")

	if flags.Has(IfNotExists) {
		conditionExpressions = append(conditionExpressions, "(attribute_not_exists(#pk))")
	}
	if flags.Has(IfAlreadyExists) {
		conditionExpressions = append(conditionExpressions, "(attribute_exists(#pk))")
	}
	if !flags.Has(KeepTTL) {
		setClauses = append(setClauses, "#ttl = :ttl")
	}

	_, err = rc.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       aws.String(strings.Join(conditionExpressions, " AND ")),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: item.eav(),
		UpdateExpression:          aws.String("SET " + strings.Join(setClauses, " , ")),
		Key: keyDef{
			pk: key,
			sk: "0",
		}.toAV(),
		ReturnValues: "",
		TableName:    aws.String(rc.table),
	}).Send(context.TODO())
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return false, nil
			}
		}
		return
	}
	return true, nil
}

// SETNX conforms to https://redis.io/commands/setnx
func (rc RedimoClient) SETNX(key string, value Value, ttl int64) (ok bool, err error) {
	return rc.SET(key, value, ttl, Flags{IfNotExists})
}

// SETEX conforms to https://redis.io/commands/setex
func (rc RedimoClient) SETEX(key string, value Value, ttl int64) (err error) {
	_, err = rc.SET(key, value, ttl, Flags{})
	return
}

// GETSET https://redis.io/commands/getset
func (rc RedimoClient) GETSET(key string, value Value) (oldValue Value, err error) {
	resp, err := rc.client.PutItemRequest(&dynamodb.PutItemInput{
		ExpressionAttributeNames: expressionAttributeNames,
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":val": value.toAV(),
		},
		Item:         keyDef{pk: key, sk: "0"}.toAV(),
		ReturnValues: dynamodb.ReturnValueAllOld,
		TableName:    aws.String(rc.table),
	}).Send(context.TODO())
	if err == nil {
		oldValue = parseItem(resp.PutItemOutput.Attributes).val
	}
	return
}

// MGET conforms to https://redis.io/commands/mget
func (rc RedimoClient) MGET(keys []string) (outputs []Value, err error) {
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
func (rc RedimoClient) MSET(data map[string]Value) (err error) {
	_, err = rc._mset(data, Flags{})
	return
}

// MSETNX conforms to https://redis.io/commands/msetnx
func (rc RedimoClient) MSETNX(data map[string]Value) (ok bool, err error) {
	return rc._mset(data, Flags{IfNotExists})
}

func (rc RedimoClient) _mset(data map[string]Value, flags Flags) (ok bool, err error) {
	var inputs []dynamodb.TransactWriteItem
	var condition *string
	if flags.Has(IfNotExists) {
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
func (rc RedimoClient) INCRBYFLOAT(key string, delta *big.Float) (after *big.Float, err error) {
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
func (rc RedimoClient) INCR(key string) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, big.NewFloat(1.0))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// DECR https://redis.io/commands/decr
func (rc RedimoClient) DECR(key string) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, big.NewFloat(-1.0))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// INRCBY https://redis.io/commands/incrby
func (rc RedimoClient) INCRBY(key string, delta *big.Int) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, new(big.Float).SetInt(delta))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}

// DECRBY https://redis.io/commands/decrby
func (rc RedimoClient) DECRBY(key string, delta *big.Int) (after *big.Int, err error) {
	floatAfter, err := rc.INCRBYFLOAT(key, new(big.Float).SetInt(new(big.Int).Neg(delta)))
	if err == nil {
		floatAfter.Int(after)
	}
	return
}
