package redimo

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"math/big"
	"strconv"
	"strings"
	"time"
)

type RedimoClient struct {
	client            *dynamodb.Client
	strongConsistency bool
	table             string
}

const pk = "pk"
const sk = "sk"
const vk = "val"
const tk = "ttl"
const defaultSK = "."

type expressionBuilder struct {
	conditions []string
	clauses    map[string][]string
	keys       map[string]struct{}
	values     map[string]dynamodb.AttributeValue
}

func (b *expressionBuilder) SET(clause string, key string, val dynamodb.AttributeValue) {
	b.clauses["SET"] = append(b.clauses["SET"], clause)
	b.keys[key] = struct{}{}
	b.values[key] = val
}

func (b *expressionBuilder) condition(condition string, references ...string) {
	b.conditions = append(b.conditions, condition)
	for _, ref := range references {
		b.keys[ref] = struct{}{}
	}
}

func (b *expressionBuilder) conditionExpression() *string {
	if len(b.conditions) == 0 {
		return nil
	}
	return aws.String(strings.Join(b.conditions, ","))
}

func (b *expressionBuilder) expressionAttributeNames() map[string]string {
	if len(b.keys) == 0 {
		return nil
	}
	out := make(map[string]string)
	for n := range b.keys {
		out["#"+n] = n
	}
	return out
}

func (b *expressionBuilder) expressionAttributeValues() map[string]dynamodb.AttributeValue {
	if len(b.values) == 0 {
		return nil
	}
	out := make(map[string]dynamodb.AttributeValue)
	for k, v := range b.values {
		out[":"+k] = v
	}
	return out
}

func (b *expressionBuilder) updateExpression() *string {
	if len(b.clauses) == 0 {
		return nil
	}
	var clauses []string
	for k, v := range b.clauses {
		clauses = append(clauses, k+" "+strings.Join(v, ", "))
	}
	return aws.String(strings.Join(clauses, " "))
}

func (b *expressionBuilder) addValue(k string, v dynamodb.AttributeValue) {
	b.keys[k] = struct{}{}
	b.values[k] = v
}

func newExpresionBuilder() expressionBuilder {
	return expressionBuilder{
		conditions: []string{},
		clauses:    make(map[string][]string),
		keys:       make(map[string]struct{}),
		values:     make(map[string]dynamodb.AttributeValue),
	}
}

var expressionAttributeNames = map[string]string{
	"#pk":    "pk",
	"#sk":    "sk",
	"#val":   "val",
	"#ttl":   "ttl",
	"#score": "score",
}

type keyDef struct {
	pk string
	sk string
}

func (k keyDef) toAV() map[string]dynamodb.AttributeValue {
	return map[string]dynamodb.AttributeValue{
		pk: {
			S: aws.String(k.pk),
		},
		sk: {
			S: aws.String(k.sk),
		},
	}
}

type itemDef struct {
	keyDef
	val   Value
	score float64
	ttl   *time.Time
}

func (i itemDef) eav() map[string]dynamodb.AttributeValue {
	eav := make(map[string]dynamodb.AttributeValue)
	eav["val"] = i.val.toAV()

	// Can't check if scores or ttl are nil, just assign them, the operation can choose to ignore the score if not applicable
	eav["score"] = dynamodb.AttributeValue{
		N: aws.String(fmt.Sprintf("%g", i.score)),
	}
	if i.ttl != nil {
		eav["ttl"] = dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(int(i.ttl.Unix()))),
		}
	} else {
		eav["ttl"] = dynamodb.AttributeValue{
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
		ttlInt, _ := strconv.ParseInt(aws.StringValue(avm["ttl"].N), 10, 64)
		ut := time.Unix(ttlInt, 0)
		item.ttl = &ut
	}

	if avm["val"].N != nil {
		num, _, _ := new(big.Float).Parse(aws.StringValue(avm["val"].N), 10)
		item.val = NumericValue{bf: num}
	} else if avm["val"].S != nil {
		item.val = StringValue{str: aws.StringValue(avm["val"].S)}
	} else if avm["val"].B != nil {
		item.val = BytesValue{bytes: avm["val"].B}
	}

	return
}

type Flag string

const (
	IfAlreadyExists Flag = "XX"
	IfNotExists     Flag = "NX"
	KeepTTL         Flag = "KEEPTTL"
)

type Flags []Flag

func (flags Flags) has(flag Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}
	return false
}

func conditionFailureError(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case dynamodb.ErrCodeConditionalCheckFailedException:
			return true
		}
	}
	return false
}
