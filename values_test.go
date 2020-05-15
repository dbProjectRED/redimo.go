package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues(t *testing.T) {
	sv := StringValue2{"hello"}
	assert.Equal(t, "hello", ReturnValue{sv.ToAV()}.String())

	iv := IntValue{42}
	assert.Equal(t, int64(42), ReturnValue{iv.ToAV()}.Int())

	fv := FloatValue{3.14}
	assert.InDelta(t, 3.14, ReturnValue{fv.ToAV()}.Float(), 0.001)

	bv := BytesValue2{[]byte{1, 2, 3, 4}}
	assert.Equal(t, []byte{1, 2, 3, 4}, ReturnValue{bv.ToAV()}.Bytes())
}
