package redimo

import (
	"context"
	"crypto/rand"
	"log"

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

func (ln listNode) nextAddressFrom(side Side) (address string) {
	switch side {
	case Left:
		address = ln.right
	case Right:
		address = ln.left
	}
	return
}

func (ln listNode) nextAttributeNameFrom(side Side) (attribute string) {
	switch side {
	case Left:
		attribute = skRight
	case Right:
		attribute = skLeft
	}
	return
}

func (ln listNode) prevAttributeNameFrom(side Side) (attribute string) {
	switch side {
	case Left:
		attribute = skLeft
	case Right:
		attribute = skRight
	}
	return
}

func (ln *listNode) setNextFrom(side Side, address string) {
	switch side {
	case Left:
		ln.right = address
	case Right:
		ln.left = address
	}
}

func (ln *listNode) setPrevFrom(side Side, address string) {
	switch side {
	case Left:
		ln.left = address
	case Right:
		ln.right = address
	}
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
	return c.listPop(key, Left)
}

func (c Client) listPop(key string, side Side) (element string, ok bool, err error) {
	endNode, ok, err := c.listEnd(key, side)
	if err != nil || !ok {
		return
	}

	var transactItems []dynamodb.TransactWriteItem

	penultimateNodeAddress := endNode.nextAddressFrom(side)
	if penultimateNodeAddress != NULL {
		penultimateKeyNode := listNode{key: key, address: penultimateNodeAddress}
		penultimateNodeUpdater := newExpresionBuilder()
		penultimateNodeUpdater.conditionEquality(penultimateKeyNode.prevAttributeNameFrom(side), StringValue{endNode.address})
		penultimateNodeUpdater.updateSET(penultimateKeyNode.prevAttributeNameFrom(side), StringValue{NULL})
		log.Println("k2y", penultimateKeyNode.key, "add:", penultimateKeyNode.address)
		transactItems = append(transactItems, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       penultimateNodeUpdater.conditionExpression(),
				ExpressionAttributeNames:  penultimateNodeUpdater.expressionAttributeNames(),
				ExpressionAttributeValues: penultimateNodeUpdater.expressionAttributeValues(),
				Key:                       penultimateKeyNode.keyAV(),
				TableName:                 aws.String(c.table),
				UpdateExpression:          penultimateNodeUpdater.updateExpression(),
			},
		})
	}
	transactItems = append(transactItems, dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:       endNode.keyAV(),
			TableName: aws.String(c.table),
		},
	})

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}).Send(context.TODO())
	if err != nil {
		return element, ok, err
	}
	return endNode.value, true, nil

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

func (c Client) listPush(key string, element string, side Side) error {
	var transactionItems []dynamodb.TransactWriteItem
	node := listNode{
		key:   key,
		value: element,
	} // need to set address, left and right.

	currentEndNode, existingList, err := c.listEnd(key, side)
	if err != nil {
		return err
	}

	if existingList {
		node.address = ulid.MustNew(ulid.Now(), rand.Reader).String()
		node.setNextFrom(side, currentEndNode.address)
		node.setPrevFrom(side, NULL)

		currentEndNodeUpdater := newExpresionBuilder()
		currentEndNodeUpdater.conditionEquality(currentEndNode.prevAttributeNameFrom(side), StringValue{NULL})
		currentEndNodeUpdater.updateSET(currentEndNode.prevAttributeNameFrom(side), StringValue{node.address})
		transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       currentEndNodeUpdater.conditionExpression(),
				ExpressionAttributeNames:  currentEndNodeUpdater.expressionAttributeNames(),
				ExpressionAttributeValues: currentEndNodeUpdater.expressionAttributeValues(),
				Key:                       currentEndNode.keyAV(),
				TableName:                 aws.String(c.table),
				UpdateExpression:          currentEndNodeUpdater.updateExpression(),
			},
		})
	} else {
		// start the list with a constant address - this prevents multiple calls from overwriting it
		node.address = key
		node.left = NULL
		node.right = NULL
	}

	nodePutter := newExpresionBuilder()
	nodePutter.addConditionNotExists(pk)
	transactionItems = append(transactionItems, dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			ConditionExpression:       nodePutter.conditionExpression(),
			ExpressionAttributeNames:  nodePutter.expressionAttributeNames(),
			ExpressionAttributeValues: nodePutter.expressionAttributeValues(),
			Item:                      node.toAV(),
			TableName:                 aws.String(c.table),
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
	node.key = key
	queryCondition := newExpresionBuilder()
	queryCondition.conditionEquality(pk, StringValue{node.key})
	queryCondition.conditionEquality(node.prevAttributeNameFrom(side), StringValue{NULL})
	resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(true),
		ExpressionAttributeNames:  queryCondition.expressionAttributeNames(),
		ExpressionAttributeValues: queryCondition.expressionAttributeValues(),
		IndexName:                 c.getIndex(node.prevAttributeNameFrom(side)),
		KeyConditionExpression:    queryCondition.conditionExpression(),
		Limit:                     aws.Int64(1),
		TableName:                 aws.String(c.table),
	}).Send(context.TODO())
	if err != nil || len(resp.Items) == 0 {
		return
	}
	found = true
	node = parseListNode(resp.Items[0])
	return c.listGet(key, node.address)
}

func (c Client) listGet(key string, address string) (node listNode, found bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: listNode{
			key:     key,
			address: address,
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

	var headAddress string

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
			if node.left == NULL {
				headAddress = node.address
			}
		}
	}

	if len(nodeMap) == 0 {
		return
	}

	runner, found := nodeMap[headAddress]
	for found {
		elements = append(elements, runner.value)
		runner, found = nodeMap[runner.right]
	}

	return
}

func (c Client) LREM(key string, count int64, elemenat string) (removedCount int64, err error) {
	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	return
}

func (c Client) LTRIM(key string, start, stop int64) (ok bool, err error) {
	return
}

func (c Client) RPOP(key string) (element string, ok bool, err error) {
	return c.listPop(key, Right)
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
