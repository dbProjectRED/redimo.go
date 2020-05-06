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

func (c Client) LPUSH(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Left)
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

// When a node is added to either end, three actions must always be performed.
// 1. The end-cap must be created or updated
// 2. The node must be inserted with the one side pointing at the end cap
// 3. The side of the node needs to be updated to point at the node.
// 3.a. If this is a new list, the other-end cap needs to be created
func (c Client) listPush(key string, element string, side Side) error {
	var transactionItems []dynamodb.TransactWriteItem
	node := listNode{
		key:     key,
		address: ulid.MustNew(ulid.Now(), rand.Reader).String(),
		value:   element,
	}

	switch side {
	case Left:
		node.left = HEAD
	case Right:
		node.right = TAIL
	}

	endNode, existingList, err := c.listEnd(key, side)

	if err != nil {
		return err
	}
	if existingList {
		var endNodeConcurrencyCheck string
		var sideToUpdateOnEndNode string
		var sideToUpdateOnExistingNode string
		var existingNodeAddress string
		switch side {
		case Left:
			sideToUpdateOnEndNode = skRight
			endNodeConcurrencyCheck = endNode.right
			node.right = endNode.right

			sideToUpdateOnExistingNode = skLeft
			existingNodeAddress = endNode.right

		case Right:
			sideToUpdateOnEndNode = skLeft
			endNodeConcurrencyCheck = endNode.left
			node.left = endNode.left

			sideToUpdateOnExistingNode = skRight
			existingNodeAddress = endNode.left
		}

		endNodeUpdateBuilder := newExpresionBuilder()
		endNodeUpdateBuilder.conditionEquality(sideToUpdateOnEndNode, StringValue{endNodeConcurrencyCheck})
		endNodeUpdateBuilder.updateSET(sideToUpdateOnEndNode, StringValue{node.address})

		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       endNodeUpdateBuilder.conditionExpression(),
				ExpressionAttributeNames:  endNodeUpdateBuilder.expressionAttributeNames(),
				ExpressionAttributeValues: endNodeUpdateBuilder.expressionAttributeValues(),
				Key:                       endNode.keyAV(),
				TableName:                 aws.String(c.table),
				UpdateExpression:          endNodeUpdateBuilder.updateExpression(),
			},
		})

		oldNodeUpdateBuilder := newExpresionBuilder()
		oldNodeUpdateBuilder.updateSET(sideToUpdateOnExistingNode, StringValue{node.address})
		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       oldNodeUpdateBuilder.conditionExpression(),
				ExpressionAttributeNames:  oldNodeUpdateBuilder.expressionAttributeNames(),
				ExpressionAttributeValues: oldNodeUpdateBuilder.expressionAttributeValues(),
				Key: listNode{
					key:     key,
					address: existingNodeAddress,
				}.keyAV(),
				TableName:        aws.String(c.table),
				UpdateExpression: oldNodeUpdateBuilder.updateExpression(),
			},
		})
	} else {
		endNode := listNode{
			key:   key,
			value: NULL,
		}
		otherEndNode := listNode{
			key:   key,
			value: NULL,
		}
		switch side {
		case Left:
			endNode.address = HEAD
			endNode.left = NULL
			endNode.right = node.address

			otherEndNode.address = TAIL
			otherEndNode.left = node.address
			otherEndNode.right = NULL

		case Right:
			endNode.address = TAIL
			endNode.left = node.address
			endNode.right = NULL

			otherEndNode.address = HEAD
			otherEndNode.right = NULL
			otherEndNode.left = node.address
		}
		node.left = HEAD
		node.right = TAIL

		nonExistenceConditionBuilder := newExpresionBuilder()
		nonExistenceConditionBuilder.conditionNonExistence(pk)
		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				ConditionExpression:       nonExistenceConditionBuilder.conditionExpression(),
				ExpressionAttributeNames:  nonExistenceConditionBuilder.expressionAttributeNames(),
				ExpressionAttributeValues: nonExistenceConditionBuilder.expressionAttributeValues(),
				Item:                      endNode.toAV(),
				TableName:                 aws.String(c.table),
			},
		})
		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				ConditionExpression:       nonExistenceConditionBuilder.conditionExpression(),
				ExpressionAttributeNames:  nonExistenceConditionBuilder.expressionAttributeNames(),
				ExpressionAttributeValues: nonExistenceConditionBuilder.expressionAttributeValues(),
				Item:                      otherEndNode.toAV(),
				TableName:                 aws.String(c.table),
			},
		})
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
		return err
	}
	return nil
}

func (c Client) listEnd(key string, side Side) (node listNode, found bool, err error) {
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
	for _, element := range elements {
		err = c.listPush(key, element, Right)
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) RPUSHX(key string, elements ...string) (newLength int64, err error) {
	return
}
