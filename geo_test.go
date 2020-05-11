package redimo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestLatLonEncoding(t *testing.T) {
	l := Location{
		Lat: 38.115556,
		Lon: 13.361389,
	}
	assert.Equal(t, "1376383545825912065", l.S2CellID())
	assert.Equal(t, "sqc8b49rnyte", l.Geohash())
	assert.Equal(t, "1376383545825912065", aws.StringValue(l.ToAV().N))
}

func TestGeoBasics(t *testing.T) {
	c := newClient(t)
	_, err := c.GEOADD("Sicily", map[string]Location{
		"Palermo": {38.115556, 13.361389},
		"Catania": {37.502669, 15.087269},
	})
	assert.NoError(t, err)

	count, err := c.ZCARD("Sicily")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	geohashes, err := c.GEOHASH("Sicily", "Palermo")
	assert.NoError(t, err)
	assert.Equal(t, []string{"sqc8b49rnyte"}, geohashes)

	geohashes, err = c.GEOHASH("Sicily", "Palermo", "Catania")
	assert.NoError(t, err)
	assert.Equal(t, []string{"sqc8b49rnyte", "sqdtr74hyu5n"}, geohashes)
}
