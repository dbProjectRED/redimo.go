package redimo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/mmcloughlin/geohash"
)

const earthRadiusMeters = 6372797.560856

type GLocation struct {
	Lat float64
	Lon float64
}

func (l GLocation) s2CellID() string {
	return fmt.Sprintf("%d", s2.CellIDFromLatLng(l.s2LatLng()))
}

func (l GLocation) Geohash() string {
	return geohash.Encode(l.Lat, l.Lon)
}

func (l GLocation) toAV() dynamodb.AttributeValue {
	return dynamodb.AttributeValue{
		N: aws.String(l.s2CellID()),
	}
}

func (l *GLocation) setCellIDString(cellIDStr string) {
	cellID, _ := strconv.ParseUint(cellIDStr, 10, 64)
	s2Cell := s2.CellID(cellID)
	s2LatLon := s2Cell.LatLng()
	l.Lat = s2LatLon.Lat.Degrees()
	l.Lon = s2LatLon.Lng.Degrees()
}

func (l GLocation) DistanceTo(other GLocation, unit GUnit) (distance float64) {
	return Meters.To(unit, l.s2LatLng().Distance(other.s2LatLng()).Radians()*earthRadiusMeters)
}

func (l GLocation) s2LatLng() s2.LatLng {
	return s2.LatLngFromDegrees(l.Lat, l.Lon)
}

func fromCellIDString(cellID string) (l GLocation) {
	l.setCellIDString(cellID)
	return
}

type GUnit float64

func (from GUnit) To(to GUnit, d float64) float64 {
	return (d / float64(to)) * float64(from)
}

const (
	Meters     GUnit = 1.0
	Kilometers GUnit = 1000.0
	Miles      GUnit = 1609.34
	Feet       GUnit = 0.3048
)

// GEOADD adds the given members into the key. Members are represented by a map of name to GLocation, which is just a wrapper
// for latitude and longitude. If a member already exists, its location will be updated. The method only returns the members
// that were added as part of the operation and did not already exist.
//
// Cost is O(1) / 1 WCU for each member being added or updated.
//
// Works similar to https://redis.io/commands/geoadd
func (c Client) GEOADD(key string, members map[string]GLocation) (newlyAddedMembers map[string]GLocation, err error) {
	newlyAddedMembers = make(map[string]GLocation)

	for member, location := range members {
		builder := newExpresionBuilder()
		builder.updateSetAV(c.skN, location.toAV())

		resp, err := c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key:                       keyDef{pk: key, sk: member}.toAV(c),
			ReturnValues:              dynamodb.ReturnValueAllOld,
			TableName:                 aws.String(c.table),
			UpdateExpression:          builder.updateExpression(),
		}).Send(context.TODO())

		if err != nil {
			return newlyAddedMembers, err
		}

		if len(resp.Attributes) < 1 {
			newlyAddedMembers[member] = location
		}
	}

	return newlyAddedMembers, nil
}

// GEODIST returns the scalar distance between the two members, converted to the given unit. If either of
// the members or the key is missing, ok will be false. Each GUnit also has convenience methods to convert
// distances into other units.
//
// Cost is O(1) / 1 RCU for each of the two members.
//
// Works similar to https://redis.io/commands/geodist
func (c Client) GEODIST(key string, member1, member2 string, unit GUnit) (distance float64, ok bool, err error) {
	locations, err := c.GEOPOS(key, member1, member2)
	if err != nil || len(locations) < 2 {
		return
	}

	return locations[member1].DistanceTo(locations[member2], unit), true, nil
}

// GEOHASH returns the Geohash strings (see https://en.wikipedia.org/wiki/Geohash) of the given members. If any members
// were not found, they will not be present in the returned map.
//
// Cost is O(1) / 1 RCU for each member.
//
// Works similar to https://redis.io/commands/geohash
func (c Client) GEOHASH(key string, members ...string) (geohashes map[string]string, err error) {
	geohashes = make(map[string]string)
	locations, err := c.GEOPOS(key, members...)

	for _, member := range members {
		if location, found := locations[member]; found {
			geohashes[member] = location.Geohash()
		}
	}

	return
}

// GEOPOS returns the stored locations for each of the given members, as a map of member to location.
// If a member cannot be found, it will not be present in the returned map.
//
// Cost is O(1) / 1 RCU for each member.
//
// Works similar to https://redis.io/commands/geopos
func (c Client) GEOPOS(key string, members ...string) (locations map[string]GLocation, err error) {
	locations = make(map[string]GLocation)

	for _, member := range members {
		resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
			ConsistentRead: aws.Bool(c.consistentReads),
			Key:            keyDef{pk: key, sk: member}.toAV(c),
			TableName:      aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return locations, err
		}

		if len(resp.Item) > 0 {
			locations[member] = fromCellIDString(aws.StringValue(resp.Item[c.skN].N))
		}
	}

	return
}

// GEORADIUS returns the members (limited to the given count) that are located within the given radius
// of the given center. Note that the positions are returned in no particular order – if there are more
// members inside the given radius than the given count, the method *does not* guarantee that the returned
// locations are the closest.
//
// The GLocation type has convenience methods to calculate the distance between points, this can be used
// to sort the locations as required.
//
// Cost is O(N) where N is the number of locations inside the square / bounding box that contains the circle
// we're searching inside.
//
// Works similar to https://redis.io/commands/georadius
func (c Client) GEORADIUS(key string, center GLocation, radius float64, radiusUnit GUnit, count int64) (positions map[string]GLocation, err error) {
	positions = make(map[string]GLocation)
	radiusCap := s2.CapFromCenterAngle(s2.PointFromLatLng(center.s2LatLng()), s1.Angle(radiusUnit.To(Meters, radius)/earthRadiusMeters))

	for _, cellID := range radiusCap.CellUnionBound() {
		builder := newExpresionBuilder()
		builder.addConditionEquality(c.pk, StringValue{key})
		builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", c.skN), c.skN)
		builder.values["start"] = dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", cellID.RangeMin()))}
		builder.values["stop"] = dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", cellID.RangeMax()))}

		var cursor map[string]dynamodb.AttributeValue

		hasMoreResults := true

		for hasMoreResults && count > 0 {
			resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
				ConsistentRead:            aws.Bool(c.consistentReads),
				ExclusiveStartKey:         cursor,
				ExpressionAttributeNames:  builder.expressionAttributeNames(),
				ExpressionAttributeValues: builder.expressionAttributeValues(),
				IndexName:                 aws.String(c.index),
				KeyConditionExpression:    builder.conditionExpression(),
				Limit:                     aws.Int64(count),
				TableName:                 aws.String(c.table),
			}).Send(context.TODO())
			if err != nil {
				return positions, err
			}

			if len(resp.LastEvaluatedKey) > 0 {
				cursor = resp.LastEvaluatedKey
			} else {
				hasMoreResults = false
			}

			for _, item := range resp.Items {
				location := fromCellIDString(aws.StringValue(item[c.skN].N))
				member := aws.StringValue(item[c.sk].S)

				if center.DistanceTo(location, radiusUnit) <= radius {
					positions[member] = location
					count--
				}
			}
		}
	}

	return
}

// GEORADIUSBYMEMBER returns the members (limited to the given count) that are located within the given radius
// of the given member. Note that the positions are returned in no particular order – if there are more
// members inside the given radius than the given count, the method *does not* guarantee that the returned
// locations are the closest.
//
// The GLocation type has convenience methods to calculate the distance between points, this can be used
// to sort the locations as required.
//
// Cost is O(N) where N is the number of locations inside the square / bounding box that contains the circle
// we're searching inside.
//
// Works similar to https://redis.io/commands/georadiusbymember
func (c Client) GEORADIUSBYMEMBER(key string, member string, radius float64, radiusUnit GUnit, count int64) (positions map[string]GLocation, err error) {
	locations, err := c.GEOPOS(key, member)
	if err == nil {
		positions, err = c.GEORADIUS(key, locations[member], radius, radiusUnit, count)
	}

	return
}
