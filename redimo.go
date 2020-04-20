package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"math/big"
	"strconv"
	"strings"
)

type keyDef struct {
	pk string
	sk string
}

func (k keyDef) toAV() map[string]dynamodb.AttributeValue {
	return map[string]dynamodb.AttributeValue{
		"pk": {
			S: aws.String(k.pk),
		},
		"sk": {
			S: aws.String(k.sk),
		},
	}
}

func (k keyDef) doesNotExist() expression.ConditionBuilder {
	return expression.Name("pk").AttributeNotExists()
}

func (k keyDef) alreadyExists() expression.ConditionBuilder {
	return expression.Name("pk").AttributeExists()
}

type itemDef struct {
	keyDef
	val     []byte
	numVal  float64
	score   float64
	ttl     int64
	numeric bool
}

var expAttrNames = map[string]string{"#pk": "pk", "#sk": "sk", "#val": "val", "#ttl": "ttl", "#score": "score"}

func (i itemDef) significantValue() []byte {
	// check numeric and return the populated value
	//if i.numeric {
	//	return []byte(strconv.FormatFloat(i.numVal))
	//} else {
	return i.val
	//}
}

func (i itemDef) buildTTLUpdate(builder expression.UpdateBuilder) expression.UpdateBuilder {
	if i.ttl > 0 {
		return builder.Set(expression.Name("ttl"), expression.Value(i.ttl))
	} else {
		return builder.Remove(expression.Name("ttl"))
	}
}

func (i itemDef) eav() map[string]dynamodb.AttributeValue {
	eav := make(map[string]dynamodb.AttributeValue)
	if i.numeric {
		eav[":val"] = dynamodb.AttributeValue{
			N: aws.String(fmt.Sprintf("%g", i.numVal)),
		}
	} else {
		eav[":val"] = dynamodb.AttributeValue{
			B: i.val,
		}
	}
	// Can't check if scores or ttl are nil, just assign them, the operation can choose to ignore the score if not applicable
	eav[":score"] = dynamodb.AttributeValue{
		N: aws.String(fmt.Sprintf("%g", i.score)),
	}
	if i.ttl > 0 {
		eav[":ttl"] = dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(int(i.ttl))),
		}
	} else {
		eav[":ttl"] = dynamodb.AttributeValue{
			NULL: aws.Bool(true),
		}
	}

	return eav
}

func parseKey(avm map[string]dynamodb.AttributeValue) keyDef {
	return keyDef{
		pk: aws.StringValue(avm["pk"].S),
		sk: aws.StringValue(avm["sk"].S),
	}
}

func parseItem(avm map[string]dynamodb.AttributeValue) (item itemDef) {
	item.keyDef = parseKey(avm)
	if avm["score"].N != nil {
		item.score, _ = strconv.ParseFloat(aws.StringValue(avm["score"].N), 64)
	}
	if avm["ttl"].N != nil {
		item.ttl, _ = strconv.ParseInt(aws.StringValue(avm["ttl"].N), 10, 64)
	}
	if avm["val"].N != nil {
		item.numeric = true
		item.numVal, _ = strconv.ParseFloat(aws.StringValue(avm["val"].N), 64)
	}
	if avm["val"].B != nil {
		item.numeric = false
		item.val = avm["val"].B
	}

	return
}

func redimoInt(intStr string) int64 {
	n, _ := strconv.ParseInt(intStr, 10, 64)
	return n
}

func redimoNum(numStr string) *big.Float {
	n, _, _ := big.ParseFloat(numStr, 10, 17, big.ToNearestEven)
	return n
}

type RedimoClient struct {
	client            *dynamodb.Client
	strongConsistency bool
	table             string
}

type Flag string

const (
	IfAlreadyExists Flag = "XX"
	IfNotExists     Flag = "NX"
	KeepTTL         Flag = "KEEPTTL"
)

type Flags []Flag

func (flags Flags) Has(flag Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}
	return false
}

// GET conforms to https://redis.io/commands/get
func (rc RedimoClient) GET(key string) (val []byte, ok bool, err error) {
	resp, err := rc.client.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(rc.strongConsistency),
		Key:            keyDef{pk: key, sk: "0"}.toAV(),
		TableName:      aws.String(rc.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}
	val = parseItem(resp.Item).significantValue()
	return
}

// SET conforms to https://redis.io/commands/set
func (rc RedimoClient) SET(key string, value []byte, ttl int64, flags Flags) (ok bool, err error) {
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
		ExpressionAttributeNames:  expAttrNames,
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
func (rc RedimoClient) SETNX(key string, value []byte, ttl int64) (ok bool, err error) {
	return rc.SET(key, value, ttl, Flags{IfNotExists})
}

// SETEX conforms to https://redis.io/commands/setex
func (rc RedimoClient) SETEX(key string, value []byte, ttl int64) (err error) {
	_, err = rc.SET(key, value, ttl, Flags{})
	return
}

// STRLEN conforms to https://redis.io/commands/strlen
func (rc RedimoClient) STRLEN(key string) (size int64, err error) {
	val, ok, err := rc.GET(key)
	if ok {
		size = int64(len(string(val)))
	} else {
		size = 0
	}
	return
}
