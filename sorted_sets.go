package redimo

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c Client) ZADD(key string, scoredMembers map[string]float64, flags Flags) (count int64, err error) {
	for member, score := range scoredMembers {
		builder := newExpresionBuilder()
		builder.SET(fmt.Sprintf("#%v = :%v", sk2, sk2), sk2, StringValue{floatToLex(score)}.toAV())

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
		count++
	}

	return
}

func (c Client) ZCARD(key string) (count int64, err error) {
	return c.HLEN(key)
}

func (c Client) ZCOUNT(key string, min, max *big.Float) (count int64, err error) {
	return
}

func (c Client) ZINCRBY(key string, delta float64, member string) (newScore float64, err error) {
	return
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

func (c Client) ZRANGEBYSCORE(key string, min, max *big.Float, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZRANK(key string, member string) (rank int64, ok bool, err error) {
	return
}

func (c Client) ZREM(key string, members ...string) (count int64, err error) {
	return
}

func (c Client) ZREMRANGEBYLEX(key string, min, max string) (count int64, err error) {
	return
}

func (c Client) ZREMRANGEBYRANK(key string, start, stop int64) (count int64, err error) {
	return
}

func (c Client) ZREMRANGEBYSCORE(key string, min, max *big.Float) (count int64, err error) {
	return
}

func (c Client) ZREVRANGE(key string, start, stop int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZREVRANGEBYLEX(key string, min, max string, offset, count int64) (membersWithScores map[string]float64, err error) {
	return
}

func (c Client) ZREVRANGEBYSCORE(key string, min, max *big.Float, offset, count int64) (membersWithScores map[string]float64, err error) {
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
		score = lexToFloat(aws.StringValue(resp.Item[sk2].S))
	}

	return
}

func (c Client) ZUNIONSTORE(key string, keys []string, weights map[string]float64, flags Flags) (count int64, err error) {
	return
}
