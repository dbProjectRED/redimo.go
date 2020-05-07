package redimo

import (
	"context"
	"crypto/rand"
	"errors"

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

func (ln listNode) next(side Side) (address string) {
	switch side {
	case Left:
		address = ln.right
	case Right:
		address = ln.left
	}

	return
}

func (ln listNode) prev(side Side) (address string) {
	switch side {
	case Left:
		address = ln.left
	case Right:
		address = ln.right
	}

	return
}

func (ln listNode) prevAttr(side Side) (attribute string) {
	switch side {
	case Left:
		attribute = skLeft
	case Right:
		attribute = skRight
	}

	return
}

func (ln *listNode) setNext(side Side, address string) {
	switch side {
	case Left:
		ln.right = address
	case Right:
		ln.left = address
	}
}

func (ln *listNode) setPrev(side Side, address string) {
	switch side {
	case Left:
		ln.left = address
	case Right:
		ln.right = address
	}
}

func (ln listNode) updateBothSidesAction(newLeft string, newRight string, table string) dynamodb.TransactWriteItem {
	updater := newExpresionBuilder()
	updater.conditionEquality(skLeft, StringValue{ln.left})
	updater.conditionEquality(skRight, StringValue{ln.right})
	updater.updateSET(skLeft, StringValue{newLeft})
	updater.updateSET(skRight, StringValue{newRight})

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       updater.conditionExpression(),
			ExpressionAttributeNames:  updater.expressionAttributeNames(),
			ExpressionAttributeValues: updater.expressionAttributeValues(),
			Key:                       ln.keyAV(),
			TableName:                 aws.String(table),
			UpdateExpression:          updater.updateExpression(),
		},
	}
}

func (ln listNode) updateSide(side Side, newAddress string, table string) dynamodb.TransactWriteItem {
	updater := newExpresionBuilder()
	updater.conditionEquality(ln.prevAttr(side), StringValue{ln.prev(side)})
	updater.updateSET(ln.prevAttr(side), StringValue{newAddress})

	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       updater.conditionExpression(),
			ExpressionAttributeNames:  updater.expressionAttributeNames(),
			ExpressionAttributeValues: updater.expressionAttributeValues(),
			Key:                       ln.keyAV(),
			TableName:                 aws.String(table),
			UpdateExpression:          updater.updateExpression(),
		},
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
	element, transactItems, ok, err := c.listPopTransactionItems(key, side)
	if err != nil || !ok {
		return
	}

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}).Send(context.TODO())

	if err != nil {
		return element, ok, err
	}

	return element, true, nil
}

func (c Client) listPopTransactionItems(key string, side Side) (element string, transactItems []dynamodb.TransactWriteItem, ok bool, err error) {
	endNode, ok, err := c.listEnd(key, side)
	if err != nil || !ok {
		return
	}

	element = endNode.value

	penultimateNodeAddress := endNode.next(side)
	if penultimateNodeAddress != NULL {
		penultimateKeyNode := listNode{key: key, address: penultimateNodeAddress}
		penultimateNodeUpdater := newExpresionBuilder()
		penultimateNodeUpdater.conditionEquality(penultimateKeyNode.prevAttr(side), StringValue{endNode.address})
		penultimateNodeUpdater.updateSET(penultimateKeyNode.prevAttr(side), StringValue{NULL})

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

	return
}

func (c Client) LPUSH(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Left, Flags{})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) listPush(key string, element string, side Side, flags Flags) error {
	transactionItems, err := c.listPushTransactions(key, element, side, flags)
	if err != nil || len(transactionItems) == 0 {
		return err
	}

	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: transactionItems,
	}).Send(context.TODO())

	if err != nil {
		return err
	}

	return nil
}

func (c Client) listPushTransactions(key string, element string, side Side, flags Flags) (transactionItems []dynamodb.TransactWriteItem, err error) {
	node := listNode{
		key:   key,
		value: element,
	} // need to set address, left and right.

	currentEndNode, existingList, err := c.listEnd(key, side)
	if err != nil {
		return
	}

	if !existingList && flags != nil && flags.has(IfAlreadyExists) {
		return transactionItems, nil
	}

	if existingList {
		node.address = ulid.MustNew(ulid.Now(), rand.Reader).String()
		node.setNext(side, currentEndNode.address)
		node.setPrev(side, NULL)

		currentEndNodeUpdater := newExpresionBuilder()
		currentEndNodeUpdater.conditionEquality(currentEndNode.prevAttr(side), StringValue{NULL})
		currentEndNodeUpdater.updateSET(currentEndNode.prevAttr(side), StringValue{node.address})

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

	return
}

func (c Client) listEnd(key string, side Side) (node listNode, found bool, err error) {
	node.key = key
	queryCondition := newExpresionBuilder()
	queryCondition.conditionEquality(pk, StringValue{node.key})
	queryCondition.conditionEquality(node.prevAttr(side), StringValue{NULL})

	resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(true),
		ExpressionAttributeNames:  queryCondition.expressionAttributeNames(),
		ExpressionAttributeValues: queryCondition.expressionAttributeValues(),
		IndexName:                 c.getIndex(node.prevAttr(side)),
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
	for _, element := range elements {
		err = c.listPush(key, element, Left, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []string, err error) {
	nodeMap := make(map[string]listNode)
	// The most common case is a full fetch, so let's start with that for now.
	queryCondition := newExpresionBuilder()
	queryCondition.conditionEquality(pk, StringValue{key})

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

	switch {
	case start >= 0 && stop > 0:
		elements = elements[start : stop+1]
	case start >= 0 && stop < 0:
		elements = elements[start:(int64(len(elements)) + stop + 1)]
	case start < 0 && stop < 0:
		elements = elements[(int64(len(elements)) + start):(int64(len(elements)) + stop + 1)]
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
	if sourceKey == destinationKey {
		return c.listRotate(sourceKey)
	}

	element, popTransactionItems, ok, err := c.listPopTransactionItems(sourceKey, Right)
	if err != nil || !ok {
		return
	}

	pushTransactionItems, err := c.listPushTransactions(destinationKey, element, Left, Flags{})

	if err != nil {
		return
	}

	var transactItems []dynamodb.TransactWriteItem
	transactItems = append(transactItems, popTransactionItems...)
	transactItems = append(transactItems, pushTransactionItems...)
	_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}).Send(context.TODO())

	return
}

func (c Client) listRotate(key string) (element string, ok bool, err error) {
	var actions []dynamodb.TransactWriteItem

	rightEnd, ok, err := c.listEnd(key, Right)

	if err != nil || !ok {
		return
	}

	leftEnd, ok, err := c.listEnd(key, Left)
	if err != nil || !ok {
		return
	}

	switch {
	case rightEnd.address == leftEnd.address:
		element = rightEnd.value
		// no action to take

	case leftEnd.right == rightEnd.address:
		actions = append(actions, leftEnd.updateBothSidesAction(rightEnd.address, NULL, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(NULL, leftEnd.address, c.table))
		element = rightEnd.value

	case leftEnd.right == rightEnd.left:
		middle, ok, err := c.listGet(key, leftEnd.right)
		if err != nil {
			return element, ok, err
		}

		if !ok {
			return element, ok, errors.New("concurrent modification")
		}

		actions = append(actions, leftEnd.updateBothSidesAction(rightEnd.address, middle.address, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(NULL, leftEnd.address, c.table))
		actions = append(actions, middle.updateBothSidesAction(leftEnd.address, NULL, c.table))
		element = rightEnd.value

	default:
		penultimateRight, ok, err := c.listGet(key, rightEnd.left)
		if err != nil {
			return element, ok, err
		}

		if !ok {
			return element, ok, errors.New("concurrent modification")
		}

		actions = append(actions, leftEnd.updateSide(Left, rightEnd.address, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(NULL, leftEnd.address, c.table))
		actions = append(actions, penultimateRight.updateSide(Right, NULL, c.table))
		element = rightEnd.value
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
	}

	return
}

func (c Client) RPUSH(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Right, nil)
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) RPUSHX(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Right, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}
