package redimo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c Client) SADD(key string, members ...string) (err error) {
	writeItems := make([]dynamodb.WriteRequest, len(members))
	for i, member := range members {
		writeItems[i] = dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: keyDef{
					pk: key,
					sk: member,
				}.toAV(),
			},
		}
	}
	requestMap := map[string][]dynamodb.WriteRequest{}
	requestMap[c.table] = writeItems
	for len(requestMap) > 0 {
		resp, err := c.ddbClient.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
			RequestItems: requestMap,
		}).Send(context.TODO())
		if err != nil {
			return err
		}
		requestMap = resp.UnprocessedItems
	}
	return
}

func (c Client) SCARD(key string) (count int64) {
	return
}

func (c Client) SDIFF(key string, otherKeys ...string) (members []string, err error) {
	return
}

func (c Client) SDIFFSTORE(destinationKey string, sourceKeys ...string) (count int64, err error) {
	return
}

func (c Client) SINTER(key string, otherKeys ...string) (memebers []string, err error) {
	return
}

func (c Client) SINTERSTORE(destinationKey string, sourceKeys ...string) (count int64, err error) {
	return
}

func (c Client) SISMEMBER(key string, member string) (ok bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            keyDef{pk: key, sk: member}.toAV(),
		TableName:      aws.String(c.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Item) == 0 {
		return
	}
	return true, nil
}

func (c Client) SMEMBERS(key string) (members []string, err error) {
	hasMoreResults := true
	var lastEvaluatedKey map[string]dynamodb.AttributeValue
	for hasMoreResults {
		builder := newExpresionBuilder()
		builder.condition(fmt.Sprintf("#%v = :%v", pk, pk), pk)
		builder.values[pk] = dynamodb.AttributeValue{
			S: aws.String(key),
		}
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  builder.expressionAttributeNames(),
			ExpressionAttributeValues: builder.expressionAttributeValues(),
			KeyConditionExpression:    builder.conditionExpression(),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return members, err
		}
		for _, item := range resp.Items {
			parsedItem := parseItem(item)
			members = append(members, parsedItem.sk)
		}
		if len(resp.LastEvaluatedKey) > 0 {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}
	}
	return
}

func (c Client) SMOVE(sourceKey string, destinationKey string, member string) (ok bool, err error) {
	return
}

func (c Client) SPOP(key string, count int64) (members []string, err error) {
	return
}

func (c Client) SRANDMEMBER(key string, count int64) (members []string, err error) {
	return
}

func (c Client) SREM(key string, members ...string) (err error) {
	return
}

func (c Client) SUNION(key string, otherKeys ...string) (members []string, err error) {
	return
}

func (c Client) SUNIONSTORE(destinationKey string, sourceKeys ...string) (count int64, err error) {
	return
}
