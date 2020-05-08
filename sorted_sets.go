package redimo

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Aggregation string

const (
	Sum Aggregation = "SUM"
	Min Aggregation = "MIN"
	Max Aggregation = "MAX"
)

var accumulators = map[Aggregation]func(float64, float64) float64{
	Sum: func(a float64, b float64) float64 {
		return a + b
	},
	Min: func(a float64, b float64) float64 {
		if a < b {
			return a
		}
		return b
	},
	Max: func(a float64, b float64) float64 {
		if a > b {
			return a
		}
		return b
	},
}

func (c Client) ZADD(key string, membersWithScores map[string]float64, flags Flags) (savedCount int64, err error) {
	for member, score := range membersWithScores {
		builder := newExpresionBuilder()
		builder.updateSET(sk2, StringValue{floatToLex(big.NewFloat(score))})

		if flags.has(IfNotExists) {
			builder.addConditionNotExists(pk)
		}

		if flags.has(IfAlreadyExists) {
			builder.addConditionExists(pk)
		}

		_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			UpdateExpression:          builder.updateExpression(),
			Key: keyDef{
				pk: key,
				sk: member,
			}.toAV(),
			TableName: aws.String(c.table),
		}).Send(context.TODO())
		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return
		}
		savedCount++
	}

	return
}

func (c Client) ZCARD(key string) (count int64, err error) {
	return c.HLEN(key)
}

func (c Client) ZCOUNT(key string, minScore, maxScore float64) (count int64, err error) {
	return c._zGeneralCount(key, floatToLex(big.NewFloat(minScore)), floatToLex(big.NewFloat(maxScore)), sk2)
}

func (c Client) _zGeneralCount(key string, min string, max string, attribute string) (count int64, err error) {
	builder := newExpresionBuilder()
	builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
	builder.values[pk] = StringValue{key}.toAV()
	betweenRange := min != "" && max != ""

	if betweenRange {
		builder.condition(fmt.Sprintf("#%v BETWEEN :min AND :max", attribute), attribute)
	}

	if min != "" {
		builder.values["min"] = StringValue{min}.toAV()

		if !betweenRange {
			builder.condition(fmt.Sprintf("#%v >= :min", attribute), attribute)
		}
	}

	if max != "" {
		builder.values["max"] = StringValue{max}.toAV()

		if !betweenRange {
			builder.condition(fmt.Sprintf("#%v <= :max", attribute), attribute)
		}
	}

	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	var indexName *string
	if attribute == sk2 {
		indexName = aws.String("lsi_sk2")
	}

	for hasMoreResults {
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 indexName,
			KeyConditionExpression:    builder.conditionExpression(),
			Select:                    dynamodb.SelectCount,
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())

		if err != nil {
			return count, err
		}

		count += aws.Int64Value(resp.Count)

		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return
}

func (c Client) ZINCRBY(key string, member string, delta float64) (newScore float64, err error) {
	tries := 0
	for tries < 3 {
		oldScore, ok, err := c.ZSCORE(key, member)
		if err != nil {
			return newScore, err
		}

		newScore = oldScore + delta
		builder := newExpresionBuilder()
		builder.SET(fmt.Sprintf("#%v = :%v", sk2, sk2), sk2, StringValue{floatToLex(big.NewFloat(newScore))}.toAV())

		if ok {
			builder.condition(fmt.Sprintf("#%v = :existingScore", sk2), sk2)
			builder.values["existingScore"] = StringValue{floatToLex(big.NewFloat(oldScore))}.toAV()
		}

		_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key: keyDef{
				pk: key,
				sk: member,
			}.toAV(),
			TableName:        aws.String(c.table),
			UpdateExpression: builder.updateExpression(),
		}).Send(context.TODO())

		if conditionFailureError(err) {
			tries++
			continue
		}

		if err != nil {
			return newScore, err
		}

		return newScore, err
	}

	return newScore, fmt.Errorf("too much contention on %v / %v", key, member)
}

func (c Client) ZINTERSTORE(destinationKey string, sourceKeys []string, aggregation Aggregation, weights map[string]float64) (count int64, err error) {
	set, err := c.ZINTER(sourceKeys, aggregation, weights)
	if err == nil {
		count, err = c.ZADD(destinationKey, set, Flags{})
	}

	return
}

func (c Client) ZLEXCOUNT(key string, min string, max string) (count int64, err error) {
	return c._zGeneralCount(key, min, max, sk)
}

func (c Client) ZPOPMAX(key string, count int64) (membersWithScores map[string]float64, err error) {
	return c._zpop(key, count, false)
}

func (c Client) ZPOPMIN(key string, count int64) (membersWithScores map[string]float64, err error) {
	return c._zpop(key, count, true)
}

func (c Client) _zpop(key string, count int64, forward bool) (membersWithScores map[string]float64, err error) {
	membersWithScores, err = c._zGeneralRange(key, "", "", 0, count, forward, sk2)
	if err != nil {
		return
	}

	members := make([]string, 0, len(membersWithScores))

	for member := range membersWithScores {
		members = append(members, member)
	}

	_, err = c.ZREM(key, members...)

	if err != nil {
		return
	}

	return
}

func (c Client) ZRANGE(key string, start, stop int64) (membersWithScores map[string]float64, err error) {
	return c._zrange(key, start, stop, true)
}

func (c Client) _zrange(key string, start int64, stop int64, forward bool) (membersWithScores map[string]float64, err error) {
	if start < 0 && stop < 0 {
		return c._zGeneralRange(key, "", "", -stop-1, -start, !forward, sk2)
	}

	if start > 0 && stop < 0 {
		lastScore, err := c._zGeneralRange(key, "", "", -stop-1, 1, !forward, sk2)
		if err != nil {
			return membersWithScores, err
		}

		return c._zGeneralRange(key, "", floatToLex(big.NewFloat(floatValues(lastScore)[0])), start, 0, forward, sk2)
	}

	return c._zGeneralRange(key, "", "", start, stop-start+1, forward, sk2)
}

func floatValues(floatValuedMap map[string]float64) (values []float64) {
	for _, v := range floatValuedMap {
		values = append(values, v)
	}

	return
}

func (c Client) ZRANGEBYLEX(key string, min, max string, offset, count int64) (membersWithScores map[string]float64, err error) {
	return c._zGeneralRange(key, min, max, offset, count, true, sk)
}

func (c Client) ZRANGEBYSCORE(key string, min, max float64, offset, count int64) (membersWithScores map[string]float64, err error) {
	return c._zGeneralRange(key, floatToLex(big.NewFloat(min)), floatToLex(big.NewFloat(max)), offset, count, true, sk2)
}

func (c Client) _zGeneralRange(key string,
	start string, stop string,
	offset int64, count int64,
	forward bool, attribute string) (membersWithScores map[string]float64, err error) {
	membersWithScores = make(map[string]float64)
	index := int64(0)
	remainingCount := count
	hasMoreResults := true

	var lastKey map[string]dynamodb.AttributeValue

	var indexName *string

	if attribute == sk2 {
		indexName = aws.String("lsi_sk2")
	}

	for hasMoreResults {
		var queryLimit *int64
		if remainingCount > 0 {
			queryLimit = aws.Int64(remainingCount + offset - index)
		}

		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
		builder.values[pk] = StringValue{key}.toAV()

		if start != "" {
			builder.values["start"] = StringValue{start}.toAV()
		}

		if stop != "" {
			builder.values["stop"] = StringValue{stop}.toAV()
		}

		switch {
		case start != "" && stop != "":
			builder.condition(fmt.Sprintf("#%v BETWEEN :start AND :stop", attribute), attribute)
		case start != "":
			builder.condition(fmt.Sprintf("#%v >= :start", attribute), attribute)
		case stop != "":
			builder.condition(fmt.Sprintf("#%v <= :stop", attribute), attribute)
		}

		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 indexName,
			KeyConditionExpression:    builder.conditionExpression(),
			Limit:                     queryLimit,
			ScanIndexForward:          aws.Bool(forward),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return membersWithScores, err
		}

		for _, item := range resp.Items {
			if index >= offset {
				pi := parseItem(item)
				membersWithScores[pi.sk], _ = lexToFloat(pi.sk2).Float64()
				remainingCount--
			}
			index++
		}

		if len(resp.LastEvaluatedKey) > 0 && remainingCount > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}

	return membersWithScores, nil
}

func (c Client) ZRANK(key string, member string) (rank int64, ok bool, err error) {
	return c._zrank(key, member, true)
}

func (c Client) _zrank(key string, member string, forward bool) (rank int64, ok bool, err error) {
	score, ok, err := c.ZSCORE(key, member)
	if err != nil || !ok {
		return
	}

	var count int64

	if forward {
		count, err = c._zGeneralCount(key, "", floatToLex(big.NewFloat(score)), sk2)
	} else {
		count, err = c._zGeneralCount(key, floatToLex(big.NewFloat(score)), "", sk2)
	}

	if err == nil {
		rank = count - 1
	}

	return
}

func (c Client) ZREM(key string, members ...string) (count int64, err error) {
	for _, member := range members {
		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("attribute_exists(#%v)", pk), pk)

		_, err = c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			ConditionExpression:       builder.conditionExpression(),
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			Key: keyDef{
				pk: key,
				sk: member,
			}.toAV(),
			TableName: aws.String(c.table),
		}).Send(context.TODO())
		if conditionFailureError(err) {
			continue
		}

		if err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}

func (c Client) ZREMRANGEBYLEX(key string, min, max string) (count int64, err error) {
	membersWithScores, err := c.ZRANGEBYLEX(key, min, max, 0, 0)
	if err == nil {
		_, err = c.ZREM(key, membersKeys(membersWithScores)...)
	}

	return int64(len(membersWithScores)), err
}

func membersKeys(membersWithScores map[string]float64) []string {
	members := make([]string, 0, len(membersWithScores))
	for member := range membersWithScores {
		members = append(members, member)
	}

	return members
}

func (c Client) ZREMRANGEBYRANK(key string, start, stop int64) (count int64, err error) {
	membersWithScores, err := c.ZRANGE(key, start, stop)
	if err == nil {
		_, err = c.ZREM(key, membersKeys(membersWithScores)...)
	}

	return int64(len(membersWithScores)), err
}

func (c Client) ZREMRANGEBYSCORE(key string, min, max float64) (count int64, err error) {
	membersWithScores, err := c.ZRANGEBYSCORE(key, min, max, 0, 0)
	if err == nil {
		_, err = c.ZREM(key, membersKeys(membersWithScores)...)
	}

	return int64(len(membersWithScores)), err
}

func (c Client) ZREVRANGE(key string, start, stop int64) (membersWithScores map[string]float64, err error) {
	return c._zrange(key, start, stop, false)
}

func (c Client) ZREVRANGEBYLEX(key string, max, min string, offset, count int64) (membersWithScores map[string]float64, err error) {
	return c._zGeneralRange(key, min, max, offset, count, false, sk)
}

func (c Client) ZREVRANGEBYSCORE(key string, max, min float64, offset, count int64) (membersWithScores map[string]float64, err error) {
	return c._zGeneralRange(key, floatToLex(big.NewFloat(min)), floatToLex(big.NewFloat(max)), offset, count, false, sk2)
}

func (c Client) ZREVRANK(key string, member string) (rank int64, ok bool, err error) {
	return c._zrank(key, member, false)
}

func (c Client) ZSCORE(key string, member string) (score float64, ok bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: keyDef{
			pk: key,
			sk: member,
		}.toAV(),
		ProjectionExpression: aws.String(strings.Join([]string{sk2}, ", ")),
		TableName:            aws.String(c.table),
	}).Send(context.TODO())
	if err == nil && len(resp.Item) > 0 {
		ok = true
		score, _ = lexToFloat(aws.StringValue(resp.Item[sk2].S)).Float64()
	}

	return
}

func (c Client) ZUNIONSTORE(destinationKey string, sourceKeys []string, aggregation Aggregation, weights map[string]float64) (count int64, err error) {
	set, err := c.ZUNION(sourceKeys, aggregation, weights)
	if err == nil {
		count, err = c.ZADD(destinationKey, set, Flags{})
	}

	return
}

func getWeight(weights map[string]float64, key string) float64 {
	if weights == nil {
		return 1
	}

	if w, ok := weights[key]; ok {
		return w
	}

	return 1
}
func (c Client) ZUNION(sourceKeys []string, aggregation Aggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores = make(map[string]float64)

	for _, sourceKey := range sourceKeys {
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)
		if err != nil {
			return membersWithScores, err
		}

		for member, score := range currentSet {
			if existingValue, ok := membersWithScores[member]; ok {
				membersWithScores[member] = accumulators[aggregation](existingValue, score*getWeight(weights, sourceKey))
			} else {
				membersWithScores[member] = score * getWeight(weights, sourceKey)
			}
		}
	}

	return
}

func (c Client) ZINTER(sourceKeys []string, aggregation Aggregation, weights map[string]float64) (membersWithScores map[string]float64, err error) {
	membersWithScores, err = c.ZRANGEBYSCORE(sourceKeys[0], math.Inf(-1), math.Inf(+1), 0, 0)
	if err != nil {
		return
	}

	for i := 1; i < len(sourceKeys); i++ {
		sourceKey := sourceKeys[i]
		currentSet, err := c.ZRANGEBYSCORE(sourceKey, math.Inf(-1), math.Inf(+1), 0, 0)

		if err != nil {
			return membersWithScores, err
		}

		for member, score := range membersWithScores {
			if currentSetValue, ok := currentSet[member]; ok {
				membersWithScores[member] = accumulators[aggregation](score, currentSetValue*getWeight(weights, sourceKey))
			} else {
				delete(membersWithScores, member)
			}
		}
	}

	return
}
