package redimo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestPointsAndDistances(t *testing.T) {
	l := GLocation{
		Lat: 38.115556,
		Lon: 13.361389,
	}
	assert.Equal(t, "1376383545825912065", l.s2CellID())
	assert.Equal(t, "sqc8b49rnyte", l.Geohash())
	assert.Equal(t, "1376383545825912065", aws.StringValue(l.toAV().N))

	assert.InDelta(t, 32.8084, Meters.To(Feet, 10), 0.01)
}

func TestGeoBasics(t *testing.T) {
	c := newClient(t)
	startingMap := map[string]GLocation{
		"Palermo": {38.115556, 13.361389},
		"Catania": {37.502669, 15.087269},
	}
	_, err := c.GEOADD("Sicily", startingMap)
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

	distance, ok, err := c.GEODIST("Sicily", "Palermo", "Catania", Meters)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.InDelta(t, 166274.1516, distance, 1)

	distance, ok, err = c.GEODIST("Sicily", "Catania", "Palermo", Kilometers)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.InDelta(t, 166.2742, distance, 0.01)

	distance, ok, err = c.GEODIST("Sicily", "Catania", "Palermo", Miles)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.InDelta(t, 103.3182, distance, 0.01)

	positions, err := c.GEOPOS("Sicily", "Palermo", "Catania")
	assert.NoError(t, err)
	assert.InDelta(t, startingMap["Palermo"].Lat, positions["Palermo"].Lat, 0.1)
	assert.InDelta(t, startingMap["Catania"].Lon, positions["Catania"].Lon, 0.1)
}

func TestGeoRadius(t *testing.T) {
	c := newClient(t)
	_, err := c.GEOADD("india", map[string]GLocation{
		"chennai":    {13.09, 80.28},
		"vellore":    {12.9204, 79.15},
		"pondy":      {11.935, 79.83},
		"bangalore":  {12.97, 77.56},
		"coimbatore": {11, 76.95},
		"madurai":    {9.939093, 78.121719},
	})
	assert.NoError(t, err)

	locations, err := c.GEORADIUS("india", GLocation{13.09, 80.28}, 180, Kilometers, 10)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(locations))
	assert.InDelta(t, locations["chennai"].Lat, 13.09, 0.1)
	assert.InDelta(t, locations["pondy"].Lon, 79.83, 0.1)
	assert.InDelta(t, locations["vellore"].Lat, 12.9204, 0.1)

	locations2, err := c.GEORADIUSBYMEMBER("india", "chennai", 180, Kilometers, 10)
	assert.NoError(t, err)
	assert.Equal(t, locations, locations2)
}
