package redimo

import (
	"math"
	"math/big"
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
}

const pk = "pk"
const sk = "sk"
const sk2 = "sk2"
const vk = "val"
const defaultSK = "/"

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

func newExpresionBuilder() expressionBuilder {
	return expressionBuilder{
		conditions: []string{},
		clauses:    make(map[string][]string),
		keys:       make(map[string]struct{}),
		values:     make(map[string]dynamodb.AttributeValue),
	}
}

type keyDef struct {
	pk  string
	sk  string
	sk2 string
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

	return m
}

type itemDef struct {
	keyDef
	val Value
}

func (i itemDef) eav() map[string]dynamodb.AttributeValue {
	eav := i.keyDef.toAV()
	eav[vk] = i.val.toAV()

	return eav
}

func parseKey(avm map[string]dynamodb.AttributeValue) keyDef {
	return keyDef{
		pk:  aws.StringValue(avm[pk].S),
		sk:  aws.StringValue(avm[sk].S),
		sk2: aws.StringValue(avm[sk2].S),
	}
}

func parseItem(avm map[string]dynamodb.AttributeValue) (item itemDef) {
	item.keyDef = parseKey(avm)

	switch {
	case avm[vk].N != nil:
		num, _, _ := new(big.Float).Parse(aws.StringValue(avm[vk].N), 10)
		item.val = NumericValue{bf: num}
	case avm[vk].S != nil:
		item.val = StringValue{str: aws.StringValue(avm[vk].S)}
	case avm[vk].B != nil:
		item.val = BytesValue{bytes: avm[vk].B}
	}

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

const expFlip = 9999
const mantissaFlip = 1

// ceeeemmmmmmmmmmmmmmmmm with the mantissa '0.' removed before big.Float guarantees < 1
func floatToLex(f *big.Float) (s string) {
	mantissa := new(big.Float)
	exp := f.MantExp(mantissa)

	var c string

	switch {
	case mantissa.Sign() > 0 && f.IsInf():
		c = "6"
		exp = 0
		mantissa = big.NewFloat(0)

	case mantissa.Sign() < 0 && f.IsInf():
		c = "0"
		exp = 0
		mantissa = big.NewFloat(0)

	case mantissa.Sign() > 0 && exp >= 0:
		c = "5"

	case mantissa.Sign() > 0 && exp < 0:
		c = "4"
		exp = expFlip + exp

	case mantissa.Sign() == 0 && exp == 0:
		c = "3"

	case mantissa.Sign() < 0 && exp < 0:
		c = "2"
		exp = -exp

		mantissa.Add(mantissa, big.NewFloat(mantissaFlip))

	case mantissa.Sign() < 0 && exp >= 0:
		c = "1"
		exp = expFlip - exp

		mantissa.Add(mantissa, big.NewFloat(mantissaFlip))
	}
	// Join after discarding the '0.' preceding the mantissa
	return strings.Join([]string{c, leftPadInt(exp, 4), mantissa.Text('f', 17)[2:]}, "")
}

func lexToFloat(lex string) (f *big.Float) {
	//ceeeemmmmmmmmmmmmmmmmm
	//01234567...
	exponent, _ := strconv.ParseInt(lex[1:5], 10, 64)
	mantissa, _, _ := new(big.Float).Parse("0."+lex[5:22], 10)

	switch lex[0] {
	case '0':
		return big.NewFloat(math.Inf(-1))
	case '1':
		return new(big.Float).SetMantExp(mantissa.Add(mantissa, big.NewFloat(-mantissaFlip)), int(expFlip-exponent))
	case '2':
		return new(big.Float).SetMantExp(mantissa.Add(mantissa, big.NewFloat(-mantissaFlip)), int(-exponent))
	case '3':
		return big.NewFloat(0)
	case '4':
		return new(big.Float).SetMantExp(mantissa, int(exponent-expFlip))
	case '5':
		return new(big.Float).SetMantExp(mantissa, int(exponent))
	case '6':
		return big.NewFloat(math.Inf(+1))
	}

	return
}

func leftPadInt(exponent int, length int) (s string) {
	s = strconv.Itoa(exponent)

	for len(s) < length {
		s = "0" + s
	}

	return
}
