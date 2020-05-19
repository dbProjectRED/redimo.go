package redimo

import (
	"math/big"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Value allows you to store values of any type supported by DynamoDB, as long as they implement this interface and encode themselves into the dynamodb.AttributeValue returned by ToAV.
//
// Some common value wrappers are provided, like StringValue, FloatValue, IntValue and BytesValue.
// The output of most operations is a ReturnValue which has convenience methods to decode the data into these common types.
//
// You can implement the ToAV() method on any type that you would like to provide a custom encoding for.
// When you receive the data wrapped in a ReturnValue, the ToAV method can be used to access
// the raw dynamo.AttributeValue struct, allowing you to do custom deserialization.
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

// ReturnValue holds a value returned by Redimo. There are convenience methods used to coerce the held value into common types,
// but you can also retrieve the raw dynamodb.AttributeValue by calling ToAV if you would like to do custom decoding.
type ReturnValue struct {
	av dynamodb.AttributeValue
}

func (rv ReturnValue) ToAV() dynamodb.AttributeValue {
	return rv.av
}

func (rv ReturnValue) String() string {
	return aws.StringValue(rv.av.S)
}

func (rv ReturnValue) Int() int64 {
	if aws.StringValue(rv.av.N) == "" {
		return 0
	}

	f, _, _ := new(big.Float).Parse(aws.StringValue(rv.av.N), 10)
	i, _ := f.Int64()

	return i
}

func (rv ReturnValue) Float() float64 {
	f, _ := strconv.ParseFloat(aws.StringValue(rv.av.N), 64)
	return f
}

func (rv ReturnValue) Bytes() []byte {
	return rv.av.B
}

func (rv ReturnValue) Empty() bool {
	return rv.av.B == nil &&
		rv.av.BOOL == nil &&
		rv.av.BS == nil &&
		rv.av.L == nil &&
		rv.av.M == nil &&
		rv.av.N == nil &&
		rv.av.NS == nil &&
		rv.av.NULL == nil &&
		rv.av.S == nil &&
		rv.av.SS == nil
}

func (rv ReturnValue) Present() bool {
	return !rv.Empty()
}

func (rv ReturnValue) Equals(ov ReturnValue) bool {
	return reflect.DeepEqual(rv.av, ov.av)
}
