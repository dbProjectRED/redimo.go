package redimo

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c Client) ZADD(key string, membersWithScores map[string]float64, flags Flags) (savedCount int64, err error) {
	for member, score := range membersWithScores {
		builder := newExpresionBuilder()
		builder.SET(fmt.Sprintf("#%v = :%v", sk2, sk2), sk2, StringValue{floatToLex(big.NewFloat(score))}.toAV())

		if flags.has(IfNotExists) {
			builder.condition(fmt.Sprintf("attribute_not_exists(#%v)", pk), pk)
		}

		if flags.has(IfAlreadyExists) {
			builder.condition(fmt.Sprintf("attribute_exists(#%v)", pk), pk)
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
	builder := newExpresionBuilder()
	builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
	builder.values[pk] = StringValue{key}.toAV()
	builder.condition(fmt.Sprintf("#%v BETWEEN :min AND :max", sk2), sk2)
	builder.values["min"] = StringValue{floatToLex(big.NewFloat(minScore))}.toAV()
	builder.values["max"] = StringValue{floatToLex(big.NewFloat(maxScore))}.toAV()
	hasMoreResults := true

	var lastEvaluatedKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			IndexName:                 aws.String("lsi_sk2"),
			KeyConditionExpression:    builder.conditionExpression(),
			ScanIndexForward:          aws.Bool(true),
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

func (c Client) ZINTERSTORE(key string, keys []string, weights map[string]float64, flags Flags) (count int64, err error) {
	return
}

func (c Client) ZLEXCOUNT(key string, min string, max string) (count int64, err error) {
	return
}

func (c Client) ZPOPMAX(key string, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZPOPMIN(key string, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZRANGE(key string, start, stop int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZRANGEBYLEX(key string, min, max string, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZRANGEBYSCORE(key string, min, max float64, minExclusive, maxExclusive bool, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZRANK(key string, member string) (rank int64, ok bool, err error) {
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
	return
}

func (c Client) ZREMRANGEBYRANK(key string, start, stop int64) (count int64, err error) {
	return
}

func (c Client) ZREMRANGEBYSCORE(key string, min, max float64) (count int64, err error) {
	return
}

func (c Client) ZREVRANGE(key string, start, stop int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZREVRANGEBYLEX(key string, min, max string, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZREVRANGEBYSCORE(key string, min, max float64, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZREVRANK(key string, member string) (rank int64, ok bool, err error) {
	return
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

func (c Client) ZUNIONSTORE(key string, keys []string, weights map[string]float64, flags Flags) (count int64, err error) {
	return
}
