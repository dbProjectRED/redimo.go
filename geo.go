package redimo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/geo/s2"
	"github.com/mmcloughlin/geohash"
)

const EarthRadiusMeters = 6372797.560856

type Location struct {
	Lat float64
	Lon float64
}

func (l Location) S2CellID() string {
	return fmt.Sprintf("%d", s2.CellIDFromLatLng(l.S2LatLng()))
}

func (l Location) Geohash() string {
	return geohash.Encode(l.Lat, l.Lon)
}

func (l Location) ToAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{
		N: aws.String(l.S2CellID()),
	}
}

func (l *Location) setCellIDString(cellIDStr string) {
	cellID, _ := strconv.ParseUint(cellIDStr, 10, 64)
	s2Cell := s2.CellID(cellID)
	s2LatLon := s2Cell.LatLng()
	l.Lat = s2LatLon.Lat.Degrees()
	l.Lon = s2LatLon.Lng.Degrees()
}

func (l Location) DistanceTo(l2 Location, unit Unit) float64 {
	return Meters.To(unit, l.S2LatLng().Distance(l2.S2LatLng()).Radians()*EarthRadiusMeters)
}

func (l Location) S2LatLng() s2.LatLng {
	return s2.LatLngFromDegrees(l.Lat, l.Lon)
}

func FromCellIDString(cellID string) (l Location) {
	l.setCellIDString(cellID)
	return
}

type Unit float64

func (from Unit) To(to Unit, d float64) float64 {
	return (d / float64(to)) * float64(from)
}

const (
	Meters     Unit = 1.0
	Kilometers Unit = 1000.0
	Miles      Unit = 1609.34
	Feet       Unit = 0.3048
)

func (c Client) GEOADD(key string, members map[string]Location) (addedCount int64, err error) {
	for member, location := range members {
		builder := newExpresionBuilder()
		builder.updateSetAV(skGeoCell, location.ToAV())

		_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(),
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		}).Send(context.TODO())

		if err != nil {
			return addedCount, err
		}
	}

	return addedCount, nil
}

func (c Client) GEODIST(key string, member1, member2 string, unit Unit) (distance float64, ok bool, err error) {
	locations, err := c.GEOPOS(key, member1, member2)
	if err != nil || len(locations) < 2 {
		return
	}

	return locations[member1].DistanceTo(locations[member2], unit), true, nil
}

func (c Client) GEOHASH(key string, members ...string) (geohashes []string, err error) {
	locations, err := c.GEOPOS(key, members...)

	for _, member := range members {
		if location, found := locations[member]; found {
			geohashes = append(geohashes, location.Geohash())
		} else {
			geohashes = append(geohashes, "")
		}
	}

	return
}

func (c Client) GEOPOS(key string, members ...string) (locations map[string]Location, err error) {
	locations = make(map[string]Location)

	for _, member := range members {
		resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
			ConsistentRead: aws.Bool(c.consistentReads),
			Key:            keyDef{pk: key, sk: member}.toAV(),
			TableName:      aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return locations, err
		}

		if len(resp.Item) > 0 {
			locations[member] = FromCellIDString(aws.StringValue(resp.Item[skGeoCell].N))
		}
	}

	return
}

func (c Client) GEORADIUS(key string, location Location, radius float64, radiusUnit Unit, count int64) (positions map[string]Location, err error) {
	return
}

func (c Client) GEORADIUSBYMEMBER(key string, member string, radius float64, radiusUnit Unit, count int64) (positions map[string]Location, err error) {
	return
}
