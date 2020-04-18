package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"math/big"
	"strconv"
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

type itemDef struct {
	keyDef
	val     []byte
	numVal  *big.Float
	score   *big.Float
	ttl     int64
	numeric bool
}

func (i itemDef) significantValue() []byte {
	if i.numeric {
		return []byte(i.numVal.String())
	} else {
		return i.val
	}
}

func parseKey(avm map[string]dynamodb.AttributeValue) keyDef {
	return keyDef{
		pk: aws.StringValue(avm["pk"].S),
		sk: aws.StringValue(avm["sk"].S),
	}
}

func parseItem(avm map[string]dynamodb.AttributeValue) itemDef {
	return itemDef{
		keyDef:  parseKey(avm),
		val:     avm["val"].B,
		numVal:  redimoNum(aws.StringValue(avm["numVal"].N)),
		score:   redimoNum(aws.StringValue(avm["score"].N)),
		ttl:     redimoInt(aws.StringValue(avm["ttl"].N)),
		numeric: !aws.BoolValue(avm["numVal"].NULL),
	}
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

func (rc RedimoClient) SET(key string, value []byte, ttl int64, flags Flags) (ok bool, err error) {
	rc.client.PutItemRequest(&dynamodb.PutItemInput{
		ConditionExpression:       nil,
		ExpressionAttributeNames:  nil,
		ExpressionAttributeValues: nil,
		Item:                      nil,
		TableName:                 aws.String(rc.table),
	})
	return
}
