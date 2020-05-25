package redimo

import (
	"context"
	"math/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type setMember struct {
	pk string
	sk string
}

func (sm setMember) toAV(c Client) map[string]dynamodb.AttributeValue {
	av := sm.keyAV(c)
	av[c.skN] = IntValue{rand.Int63()}.ToAV()

	return av
}

func (sm setMember) keyAV(c Client) map[string]dynamodb.AttributeValue {
	av := make(map[string]dynamodb.AttributeValue)
	av[c.pk] = StringValue{sm.pk}.ToAV()
	av[c.sk] = StringValue{sm.sk}.ToAV()

	return av
}

func (c Client) SADD(key string, members ...string) (addedMembers []string, err error) {
	for _, member := range members {
		resp, err := c.ddbClient.PutItemRequest(&dynamodb.PutItemInput{
			Item:         setMember{pk: key, sk: member}.toAV(c),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return addedMembers, err
		}

		if len(resp.Attributes) == 0 {
			addedMembers = append(addedMembers, member)
		}
	}

	return
}

func (c Client) SCARD(key string) (count int64, err error) {
	return c.HLEN(key)
}

func (c Client) SDIFF(key string, subtractKeys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})
	startingList, err := c.SMEMBERS(key)

	if err != nil {
		return
	}

	for _, member := range startingList {
		memberSet[member] = struct{}{}
	}

	for _, otherKey := range subtractKeys {
		otherList, err := c.SMEMBERS(otherKey)
		if err != nil {
			return members, err
		}

		for _, member := range otherList {
			delete(memberSet, member)
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SDIFFSTORE(destinationKey string, sourceKey string, subtractKeys ...string) (count int64, err error) {
	members, err := c.SDIFF(sourceKey, subtractKeys...)
	if err == nil {
		_, err = c.SADD(destinationKey, members...)
	}

	return int64(len(members)), err
}

func (c Client) SINTER(key string, otherKeys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})
	startingList, err := c.SMEMBERS(key)

	if err != nil {
		return
	}

	for _, member := range startingList {
		memberSet[member] = struct{}{}
	}

	for _, otherKey := range otherKeys {
		otherList, err := c.SMEMBERS(otherKey)
		if err != nil {
			return members, err
		}

		otherSet := make(map[string]struct{})

		for _, member := range otherList {
			otherSet[member] = struct{}{}
		}

		for member := range memberSet {
			if _, ok := otherSet[member]; !ok {
				delete(memberSet, member)
			}
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SINTERSTORE(destinationKey string, sourceKey string, otherKeys ...string) (count int64, err error) {
	members, err := c.SINTER(sourceKey, otherKeys...)
	if err == nil {
		_, err = c.SADD(destinationKey, members...)
	}

	return int64(len(members)), err
}

func (c Client) SISMEMBER(key string, member string) (ok bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key:            setMember{pk: key, sk: member}.keyAV(c),
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
		builder.addConditionEquality(c.pk, StringValue{key})

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
			parsedItem := parseItem(item, c)
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
	builder := newExpresionBuilder()
	builder.addConditionExists(c.pk)

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodb.TransactWriteItem{
			{
				Delete: &dynamodb.Delete{
					ConditionExpression:       builder.conditionExpression(),
					ExpressionAttributeNames:  builder.expressionAttributeNames(),
					ExpressionAttributeValues: builder.expressionAttributeValues(),
					Key:                       setMember{pk: sourceKey, sk: member}.keyAV(c),
					TableName:                 aws.String(c.table),
				},
			},
			{
				Put: &dynamodb.Put{
					Item:      setMember{pk: destinationKey, sk: member}.toAV(c),
					TableName: aws.String(c.table),
				},
			},
		},
	}).Send(context.TODO())

	if conditionFailureError(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c Client) SPOP(key string, count int64) (members []string, err error) {
	members, err = c.SRANDMEMBER(key, count)
	if err == nil {
		_, err = c.SREM(key, members...)
	}

	return
}

func (c Client) SRANDMEMBER(key string, count int64) (members []string, err error) {
	if count < 0 {
		count = -count
	}

	builder := newExpresionBuilder()
	builder.addConditionEquality(c.pk, StringValue{key})

	resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(c.consistentReads),
		ExpressionAttributeNames:  builder.expressionAttributeNames(),
		ExpressionAttributeValues: builder.expressionAttributeValues(),
		KeyConditionExpression:    builder.conditionExpression(),
		Limit:                     aws.Int64(count),
		TableName:                 aws.String(c.table),
	}).Send(context.TODO())

	if err != nil {
		return members, err
	}

	for _, item := range resp.Items {
		parsedItem := parseItem(item, c)
		members = append(members, parsedItem.sk)
	}

	return
}

func (c Client) SREM(key string, members ...string) (removedMembers []string, err error) {
	for _, member := range members {
		resp, err := c.ddbClient.DeleteItemRequest(&dynamodb.DeleteItemInput{
			Key: setMember{
				pk: key,
				sk: member,
			}.keyAV(c),
			ReturnValues: dynamodb.ReturnValueAllOld,
			TableName:    aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return removedMembers, err
		}

		if len(resp.Attributes) > 0 {
			removedMembers = append(removedMembers, member)
		}
	}

	return
}

func (c Client) SUNION(keys ...string) (members []string, err error) {
	memberSet := make(map[string]struct{})

	for _, key := range keys {
		setMembers, err := c.SMEMBERS(key)
		if err != nil {
			return members, err
		}

		for _, member := range setMembers {
			memberSet[member] = struct{}{}
		}
	}

	for member := range memberSet {
		members = append(members, member)
	}

	return
}

func (c Client) SUNIONSTORE(destinationKey string, sourceKeys ...string) (count int64, err error) {
	members, err := c.SUNION(sourceKeys...)
	if err == nil {
		_, err = c.SADD(destinationKey, members...)
	}

	return int64(len(members)), err
}
