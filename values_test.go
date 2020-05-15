package redimo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues(t *testing.T) {
	sv := StringValue{"hello"}
	assert.Equal(t, "hello", ReturnValue{sv.ToAV()}.String())
	assert.False(t, ReturnValue{sv.ToAV()}.Empty())

	iv := IntValue{42}
	assert.Equal(t, int64(42), ReturnValue{iv.ToAV()}.Int())
	assert.True(t, ReturnValue{iv.ToAV()}.Present())

	fv := FloatValue{3.14}
	assert.InDelta(t, 3.14, ReturnValue{fv.ToAV()}.Float(), 0.001)

	bv := BytesValue{[]byte{1, 2, 3, 4}}
	assert.Equal(t, []byte{1, 2, 3, 4}, ReturnValue{bv.ToAV()}.Bytes())

	assert.True(t, ReturnValue{}.Empty())
	assert.False(t, ReturnValue{}.Present())

	// Ensure that return value indicates presence even with empty and zero values
	assert.True(t, ReturnValue{BytesValue{[]byte{}}.ToAV()}.Present())
	assert.True(t, ReturnValue{IntValue{0}.ToAV()}.Present())
	assert.True(t, ReturnValue{StringValue{""}.ToAV()}.Present())
}
