package redimo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client struct {
	ddbClient       *dynamodb.Client
	consistentReads bool
	table           string
	indexes         map[string]string
}

func (c Client) getIndex(attribute string) *string {
	if c.indexes == nil {
		return nil
	}

	index, found := c.indexes[attribute]
	if !found {
		return nil
	}

	return aws.String(index)
}

const (
	pk   = "pk"
	sk   = "sk"
	sk2  = "sk2"
	sk3  = "sk3"
	sk4  = "sk4"
	vk   = "val"
	skN1 = "skN1"
)

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

	return aws.String(strings.Join(b.conditions, " AND "))
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

	clauses := make([]string, 0, len(b.clauses))

	for k, v := range b.clauses {
		clauses = append(clauses, k+" "+strings.Join(v, ", "))
	}

	return aws.String(strings.Join(clauses, " "))
}

func (b *expressionBuilder) addConditionEquality(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v = :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThan(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v < :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) addConditionLessThanOrEqualTo(attributeName string, value Value) {
	valueName := "cval" + strconv.Itoa(len(b.conditions))
	b.condition(fmt.Sprintf("#%v <= :%v", attributeName, valueName), attributeName)
	b.values[valueName] = value.ToAV()
}

func (b *expressionBuilder) updateSET(attributeName string, value Value) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, value.ToAV())
}

func (b *expressionBuilder) updateSetAV(attributeName string, av dynamodb.AttributeValue) {
	b.SET(fmt.Sprintf("#%v = :%v", attributeName, attributeName), attributeName, av)
}

func (b *expressionBuilder) addConditionNotExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_not_exists(#%v)", attributeName), attributeName)
}

func (b *expressionBuilder) addConditionExists(attributeName string) {
	b.condition(fmt.Sprintf("attribute_exists(#%v)", attributeName), attributeName)
}

func newExpresionBuilder() expressionBuilder {
	return expressionBuilder{
		conditions: []string{},
		clauses:    make(map[string][]string),
		keys:       make(map[string]struct{}),
		values:     make(map[string]dynamodb.AttributeValue),
	}
}

type keyDef struct {
	pk   string
	sk   string
	sk2  string
	sk3  string
	sk4  string
	skN1 string
}

func (k keyDef) toAV() map[string]dynamodb.AttributeValue {
	m := map[string]dynamodb.AttributeValue{
		pk: {
			S: aws.String(k.pk),
		},
		sk: {
			S: aws.String(k.sk),
		},
	}
	if k.sk2 != "" {
		m[sk2] = dynamodb.AttributeValue{
			S: aws.String(k.sk2),
		}
	}

	if k.sk3 != "" {
		m[sk3] = dynamodb.AttributeValue{
			S: aws.String(k.sk3),
		}
	}

	if k.sk4 != "" {
		m[sk4] = dynamodb.AttributeValue{
			S: aws.String(k.sk4),
		}
	}

	if k.skN1 != "" {
		m[skN1] = dynamodb.AttributeValue{
			N: aws.String(k.skN1),
		}
	}

	return m
}

type itemDef struct {
	keyDef
	val ReturnValue
}

func (i itemDef) eav() map[string]dynamodb.AttributeValue {
	eav := i.keyDef.toAV()
	eav[vk] = i.val.AV

	return eav
}

func parseKey(avm map[string]dynamodb.AttributeValue) keyDef {
	return keyDef{
		pk:  aws.StringValue(avm[pk].S),
		sk:  aws.StringValue(avm[sk].S),
		sk2: aws.StringValue(avm[sk2].S),
		sk3: aws.StringValue(avm[sk3].S),
		sk4: aws.StringValue(avm[sk4].S),
	}
}

func parseItem(avm map[string]dynamodb.AttributeValue) (item itemDef) {
	item.keyDef = parseKey(avm)
	item.val = ReturnValue{avm[vk]}

	return
}

type Flag string

const (
	IfAlreadyExists Flag = "XX"
	IfNotExists     Flag = "NX"
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
		case dynamodb.ErrCodeConditionalCheckFailedException,
			dynamodb.ErrCodeTransactionInProgressException,
			dynamodb.ErrCodeTransactionConflictException,
			dynamodb.ErrCodeTransactionCanceledException:
			return true
		}
	}

	return false
}
