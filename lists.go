package redimo

import (
	"context"
	"crypto/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/oklog/ulid"
)

type Side string

const (
	Left  Side = "LEFT"
	Right Side = "RIGHT"
)

func (c Client) LINDEX(key string, index int64) (element string, err error) {
	return
}

func (c Client) LINSERT(key string, side Side, pivotElement string) (newLength int64, err error) {
	return
}

func (c Client) LLEN(key string) (length int64, err error) {
	return
}

func (c Client) LPOP(key string) (element string, ok bool, err error) {
	return
}

type listNode struct {
	key     string
	address string
	left    string
	right   string
	value   string
}

const NULL = "NULL"
const HEAD = "HEAD"
const TAIL = "TAIL"

func (ln listNode) toAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = dynamodb.AttributeValue{S: aws.String(ln.key)}
	avm[sk] = dynamodb.AttributeValue{S: aws.String(ln.address)}
	avm[skLeft] = dynamodb.AttributeValue{S: aws.String(ln.left)}
	avm[skRight] = dynamodb.AttributeValue{S: aws.String(ln.right)}
	avm[vk] = dynamodb.AttributeValue{S: aws.String(ln.value)}

	return avm
}

func (ln listNode) keyAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = dynamodb.AttributeValue{S: aws.String(ln.key)}
	avm[sk] = dynamodb.AttributeValue{S: aws.String(ln.address)}

	return avm
}

func parseListNode(avm map[string]dynamodb.AttributeValue) (ln listNode) {
	ln.key = aws.StringValue(avm[pk].S)
	ln.address = aws.StringValue(avm[sk].S)
	ln.left = aws.StringValue(avm[skLeft].S)
	ln.right = aws.StringValue(avm[skRight].S)
	ln.value = aws.StringValue(avm[vk].S)

	return
}

func (c Client) LPUSH(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		var transactionItems []dynamodb.TransactWriteItem
		node := listNode{
			key:     key,
			address: ulid.MustNew(ulid.Now(), rand.Reader).String(),
			left:    HEAD,
			right:   "",
			value:   element,
		}
		newHeadNode := listNode{
			key:     key,
			address: HEAD,
			left:    NULL,
			right:   node.address,
			value:   NULL,
		}
		oldHead, found, err := c._END(key, Left)

		if err != nil {
			return newLength, err
		}
		if found {
			// if we already have a HEAD, then we need to
			// 1. set the `right` on our new node to the old node
			// 2. update the HEAD to the address of our new node
			// 3. update the old node's `left` to point to our new node

			// 1.
			node.right = oldHead.right

			// 2. Need to update the head. Should fail if another transaction has already changed it
			headUpdateBuilder := newExpresionBuilder()
			headUpdateBuilder.conditionEquality(skRight, StringValue{oldHead.right})
			headUpdateBuilder.updateSET(skRight, StringValue{node.address})

			transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
				Update: &dynamodb.Update{
					ConditionExpression:       headUpdateBuilder.conditionExpression(),
					ExpressionAttributeNames:  headUpdateBuilder.expressionAttributeNames(),
					ExpressionAttributeValues: headUpdateBuilder.expressionAttributeValues(),
					Key:                       newHeadNode.keyAV(),
					TableName:                 aws.String(c.table),
					UpdateExpression:          headUpdateBuilder.updateExpression(),
				},
			})

			// 3. also update the node  at the old head to point at the new node
			// this need not be conditional - if the head has changed we'll already fail
			oldNodeUpdateBuilder := newExpresionBuilder()
			oldNodeUpdateBuilder.updateSET(skLeft, StringValue{node.address})

			transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
				Update: &dynamodb.Update{
					ConditionExpression:       oldNodeUpdateBuilder.conditionExpression(),
					ExpressionAttributeNames:  oldNodeUpdateBuilder.expressionAttributeNames(),
					ExpressionAttributeValues: oldNodeUpdateBuilder.expressionAttributeValues(),
					Key: listNode{
						key:     key,
						address: oldHead.right,
					}.keyAV(),
					TableName:        aws.String(c.table),
					UpdateExpression: oldNodeUpdateBuilder.updateExpression(),
				},
			})
		} else {
			// if we don't have a HEAD, this is a new list - let's make a TAIL node as well. Can
			// condition it to fail if another transaction already inserts it.
			// Then we PUT the HEAD and TAIL nodes as well, with conditions to make sure they don't already exist.
			newTailNode := listNode{
				key:     key,
				address: TAIL,
				left:    node.address,
				right:   NULL,
				value:   NULL,
			}

			nonExistenceConditionBuilder := newExpresionBuilder()
			nonExistenceConditionBuilder.conditionNonExistence(pk)
			transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					ConditionExpression:       nonExistenceConditionBuilder.conditionExpression(),
					ExpressionAttributeNames:  nonExistenceConditionBuilder.expressionAttributeNames(),
					ExpressionAttributeValues: nonExistenceConditionBuilder.expressionAttributeValues(),
					Item:                      newHeadNode.toAV(),
					TableName:                 aws.String(c.table),
				},
			})
			transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
				Put: &dynamodb.Put{
					ConditionExpression:       nonExistenceConditionBuilder.conditionExpression(),
					ExpressionAttributeNames:  nonExistenceConditionBuilder.expressionAttributeNames(),
					ExpressionAttributeValues: nonExistenceConditionBuilder.expressionAttributeValues(),
					Item:                      newTailNode.toAV(),
					TableName:                 aws.String(c.table),
				},
			})

			node.right = newTailNode.address
		}
		// Let's add the node itself into the transaction, unconditionally. There's enough failure checks everywhere else.
		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				Item:      node.toAV(),
				TableName: aws.String(c.table),
			},
		})

		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: transactionItems,
		}).Send(context.TODO())
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) _END(key string, side Side) (node listNode, found bool, err error) {
	sideAddressMap := map[Side]string{
		Left:  HEAD,
		Right: TAIL,
	}
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(c.consistentReads),
		Key: listNode{
			key:     key,
			address: sideAddressMap[side],
		}.keyAV(),
		TableName: aws.String(c.table),
	}).Send(context.TODO())

	if err != nil {
		return
	}

	if len(resp.Item) > 0 {
		found = true
		node = parseListNode(resp.Item)
	}

	return
}

func (c Client) LPUSHX(key string, elements ...string) (newLength int64, err error) {
	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []string, err error) {
	// The most common case is a full fetch, so let's start with that for now.
	queryCondition := newExpresionBuilder()
	queryCondition.conditionEquality(pk, StringValue{key})

	nodeMap := make(map[string]listNode)
	hasMoreResults := true

	var lastKey map[string]dynamodb.AttributeValue

	for hasMoreResults {
		resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(c.consistentReads),
			ExclusiveStartKey:         lastKey,
			ExpressionAttributeNames:  queryCondition.expressionAttributeNames(),
			ExpressionAttributeValues: queryCondition.expressionAttributeValues(),
			KeyConditionExpression:    queryCondition.conditionExpression(),
			TableName:                 aws.String(c.table),
		}).Send(context.TODO())
		if err != nil {
			return elements, err
		}

		if len(resp.LastEvaluatedKey) > 0 {
			lastKey = resp.LastEvaluatedKey
		} else {
			hasMoreResults = false
		}

		for _, rawNode := range resp.Items {
			node := parseListNode(rawNode)
			nodeMap[node.address] = node
		}
	}

	if len(nodeMap) == 0 {
		return
	}

	delete(nodeMap, TAIL)
	runner, found := nodeMap[nodeMap[HEAD].right]

	for found {
		elements = append(elements, runner.value)
		runner, found = nodeMap[runner.right]
	}

	return
}

func (c Client) LREM(key string, count int64, element string) (removedCount int64, err error) {
	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	return
}

func (c Client) LTRIM(key string, start, stop int64) (ok bool, err error) {
	return
}

func (c Client) RPOP(key string) (element string, ok bool, err error) {
	return
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element string, ok bool, err error) {
	return
}

func (c Client) RPUSH(key string, elements ...string) (newLength int64, err error) {
	return
}

func (c Client) RPUSHX(key string, elements ...string) (newLength int64, err error) {
	return
}
