package redimo

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"math/big"
	"strconv"
)

type RedimoClient struct {
	client            *dynamodb.Client
	strongConsistency bool
	table             string
}

var expressionAttributeNames = map[string]string{
	"#pk":    "pk",
	"#sk":    "sk",
	"#val":   "val",
	"#ttl":   "ttl",
	"#score": "score",
}

func expAttrNames(names ...string) map[string]string {
	out := make(map[string]string)
	for _, name := range names {
		out["#"+name] = expressionAttributeNames["#"+name]
	}
	return out
}

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
	val   Value
	score float64
	ttl   int64
}

func (i itemDef) eav() map[string]dynamodb.AttributeValue {
	eav := make(map[string]dynamodb.AttributeValue)
	eav[":val"] = i.val.toAV()

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

func (flags Flags) Has(flag Flag) bool {
	for _, f := range flags {
		if f == flag {
			return true
		}
	}
	return false
}
