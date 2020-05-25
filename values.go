package redimo

import (
	"math/big"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Value allows you to store values of any type supported by DynamoDB, as long as they implement this interface and
// encode themselves into a dynamodb.AttributeValue returned by ToAV.
//
// Every Redimo operation that stores data will accept the data as a Value. Some common value wrappers are provided,
// like StringValue, FloatValue, IntValue and BytesValue, allowing you to easily wrap the data you store.
//
// The output of most operations is a ReturnValue which has convenience methods to decode the data into these common types.
// ReturnValue also implements Value so you can call ToAV to access the raw dynamodb.AttributeValue, allowing you to
// do custom de-serialization.
//
// If you have a data that does not fit cleanly into one of the provide convenience wrapper types, you can implement the ToAV()
// method on any type to implement custom encoding. When you receive the data wrapped in a ReturnValue, the ToAV method can
// be used to access the raw dynamo.AttributeValue struct, allowing you to do custom decoding.
type Value interface {
	ToAV() dynamodb.AttributeValue
}

// StringValue is a convenience value wrapper for a string, usable as
//   StringValue{"hello"}
type StringValue struct {
	S string
}

func (sv StringValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{S: aws.String(sv.S)}
}

// FloatValue is a convenience value wrapper for a float64, usable as
//   FloatValue{3.14}
type FloatValue struct {
	F float64
}

func (fv FloatValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(fv.F, 'G', 17, 64))}
}

// IntValue is a convenience value wrapper for an int64, usable as
//   IntValue{42}
type IntValue struct {
	I int64
}

func (iv IntValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(iv.I, 10))}
}

// BytesValue is a convenience wrapper for a byte slice, usable as
//   BytesValue{[]byte{1,2,3}}
type BytesValue struct {
	B []byte
}

func (bv BytesValue) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{B: bv.B}
}

// ReturnValue holds a value returned by DynamoDB. There are convenience methods used to coerce the held value into common types,
// but you can also retrieve the raw dynamodb.AttributeValue by calling ToAV if you would like to do custom decoding.
type ReturnValue struct {
	av dynamodb.AttributeValue
}

// ToAV returns the underlying dynamodb.AttributeValue, allow custom deserialization.
func (rv ReturnValue) ToAV() dynamodb.AttributeValue {
	return rv.av
}

// String returns the value as a string. If the value was not stored as a string, a zero-value / empty string
// will the returned. This method will not coerce numeric of byte values.
func (rv ReturnValue) String() string {
	return aws.StringValue(rv.av.S)
}

// Int returns the value as int64. Will be zero-valued if the value is not actually numeric. The value was originally
// a float, it will be truncated.
func (rv ReturnValue) Int() int64 {
	if aws.StringValue(rv.av.N) == "" {
		return 0
	}

	f, _, _ := new(big.Float).Parse(aws.StringValue(rv.av.N), 10)
	i, _ := f.Int64()

	return i
}

// Float returns the value as float64. Will be zero-valued if the value is not numeric. If the value
// was originally stored as an int, it will be converted to float64 based on parsing the string
// representation, so there is some scope for overflows being corrected silently.
func (rv ReturnValue) Float() float64 {
	f, _ := strconv.ParseFloat(aws.StringValue(rv.av.N), 64)
	return f
}

// Bytes returns the value as a byte slice. Will be nil if the value is not actually a byte slice.
func (rv ReturnValue) Bytes() []byte {
	return rv.av.B
}

// Empty returns true if the value is empty or uninitialized. This
// indicates that the underlying DynamoDB operation did not return a value.
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

// Present returns true if a value is present. It indicates that the underlying
// DynamoDB AttributeValue has a data in any one of its fields. If you already know
// the type of your value, you can call the convenience method (like String() or Int())
// or you can retrieve the underlying dynamodb.AttributeValue struct with ToAV and perform
// your down decoding.
func (rv ReturnValue) Present() bool {
	return !rv.Empty()
}

// Equals checks equality by comparing the underlying dynamodb.AttributeValues. If they
// both hold the same value, as indicated by the rules of reflect.DeepEqual, Equals will return true.
func (rv ReturnValue) Equals(ov ReturnValue) bool {
	return reflect.DeepEqual(rv.av, ov.av)
}
