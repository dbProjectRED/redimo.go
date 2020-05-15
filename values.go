package redimo

import (
	"math/big"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Value interface {
	ToAV() dynamodb.AttributeValue
}

type StringValue struct {
	S string
}

func (sv StringValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{S: aws.String(sv.S)}
}

type FloatValue struct {
	F float64
}

func (fv FloatValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(fv.F, 'G', 17, 64))}
}

type IntValue struct {
	I int64
}

func (iv IntValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(iv.I, 10))}
}

type BytesValue struct {
	B []byte
}

func (bv BytesValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{B: bv.B}
}

type ReturnValue struct {
	AV dynamodb.AttributeValue
}

func (rv ReturnValue) String() string {
	return aws.StringValue(rv.AV.S)
}

func (rv ReturnValue) Int() int64 {
	f, _, _ := new(big.Float).Parse(aws.StringValue(rv.AV.N), 10)
	i, _ := f.Int64()

	return i
}

func (rv ReturnValue) Float() float64 {
	f, _ := strconv.ParseFloat(aws.StringValue(rv.AV.N), 64)
	return f
}

func (rv ReturnValue) Bytes() []byte {
	return rv.AV.B
}

func (rv ReturnValue) Empty() bool {
	return rv.AV.B == nil &&
		rv.AV.BOOL == nil &&
		rv.AV.BS == nil &&
		rv.AV.L == nil &&
		rv.AV.M == nil &&
		rv.AV.N == nil &&
		rv.AV.NS == nil &&
		rv.AV.NULL == nil &&
		rv.AV.S == nil &&
		rv.AV.SS == nil
}

func (rv ReturnValue) Present() bool {
	return !rv.Empty()
}
