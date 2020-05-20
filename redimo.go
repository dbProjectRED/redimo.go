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
	pk  = "pk"
	sk  = "sk"
	vk  = "val"
	skN = "skN"
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
	pk string
	sk string
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

	return m
}

type itemDef struct {
	keyDef
	val ReturnValue
}

func parseKey(avm map[string]dynamodb.AttributeValue) keyDef {
	return keyDef{
		pk: aws.StringValue(avm[pk].S),
		sk: aws.StringValue(avm[sk].S),
	}
}

func parseItem(avm map[string]dynamodb.AttributeValue) (item itemDef) {
	item.keyDef = parseKey(avm)
	item.val = ReturnValue{avm[vk]}

	return
}

type Flag string

const (
	None            Flag = "-"
	Unconditionally      = None
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
