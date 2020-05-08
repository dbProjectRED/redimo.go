package redimo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/oklog/ulid"
)

type Side string

func (s Side) otherSide() (otherSide Side) {
	switch s {
	case Left:
		otherSide = Right
	case Right:
		otherSide = Left
	}

	return
}

const (
	Left  Side = "LEFT"
	Right Side = "RIGHT"
)

type lNode struct {
	key     string
	address string
	left    string
	right   string
	value   string
}

const NULL = "NULL"

func (ln lNode) toAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = dynamodb.AttributeValue{S: aws.String(ln.key)}
	avm[sk] = dynamodb.AttributeValue{S: aws.String(ln.address)}
	avm[skLeft] = dynamodb.AttributeValue{S: aws.String(ln.left)}
	avm[skRight] = dynamodb.AttributeValue{S: aws.String(ln.right)}
	avm[vk] = dynamodb.AttributeValue{S: aws.String(ln.value)}

	return avm
}

func (ln lNode) keyAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = dynamodb.AttributeValue{S: aws.String(ln.key)}
	avm[sk] = dynamodb.AttributeValue{S: aws.String(ln.address)}

	return avm
}

func (ln lNode) next(side Side) (address string) {
	switch side {
	case Left:
		address = ln.right
	case Right:
		address = ln.left
	}

	return
}

func (ln lNode) prev(side Side) (address string) {
	switch side {
	case Left:
		address = ln.left
	case Right:
		address = ln.right
	}

	return
}

func (ln lNode) prevAttr(side Side) (attribute string) {
	switch side {
	case Left:
		attribute = skLeft
	case Right:
		attribute = skRight
	}

	return
}

func (ln *lNode) setNext(side Side, address string) {
	switch side {
	case Left:
		ln.right = address
	case Right:
		ln.left = address
	}
}

func (ln *lNode) setPrev(side Side, address string) {
	switch side {
	case Left:
		ln.left = address
	case Right:
		ln.right = address
	}
}

func (ln lNode) updateBothSidesAction(newLeft string, newRight string, table string) dynamodb.TransactWriteItem {
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

func (ln lNode) updateSideAction(side Side, newAddress string, table string) dynamodb.TransactWriteItem {
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

func (ln lNode) isTail() bool {
	return ln.right == NULL
}

func (ln lNode) isHead() bool {
	return ln.left == NULL
}

func (ln lNode) putAction(table string) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      ln.toAV(),
			TableName: aws.String(table),
		},
	}
}

func (ln lNode) deleteAction(table string) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:       ln.keyAV(),
			TableName: aws.String(table),
		},
	}
}

func lParseNode(avm map[string]dynamodb.AttributeValue) (ln lNode) {
	ln.key = aws.StringValue(avm[pk].S)
	ln.address = aws.StringValue(avm[sk].S)
	ln.left = aws.StringValue(avm[skLeft].S)
	ln.right = aws.StringValue(avm[skRight].S)
	ln.value = aws.StringValue(avm[vk].S)

	return
}

func (c Client) LINDEX(key string, index int64) (element string, found bool, err error) {
	node, found, err := c.lNodeAtIndex(key, index)
	if err != nil {
		return
	}

	return node.value, found, err
}

func (c Client) lNodeAtIndex(key string, index int64) (node lNode, found bool, err error) {
	side := Left
	if index < 0 {
		side = Right
		index = -index - 1
	}

	node, found, err = c.lFindEnd(key, side)
	i := int64(0)

	for found {
		if err != nil {
			return
		}

		if i == index {
			return node, true, nil
		}

		node, found, err = c.lGetByAddress(key, node.next(side))
		i++
	}

	return node, false, nil
}

// LINSERT inserts the given element on the given side of the pivot element.
func (c Client) LINSERT(key string, side Side, pivot, element string) (newLength int64, done bool, err error) {
	var actions []dynamodb.TransactWriteItem

	pivotNode, found, err := c.listNodeAtPivot(key, pivot, Left)
	if err != nil || !found {
		return newLength, false, err
	}

	switch {
	case pivotNode.isHead() && side == Left:
		_, err = c.LPUSHX(key, element)
	case pivotNode.isTail() && side == Right:
		_, err = c.RPUSHX(key, element)
	default:
		otherNode, ok, err := c.lGetByAddress(key, pivotNode.prev(side))
		if err != nil || !ok {
			return newLength, false, fmt.Errorf("could not find or load required node %v: %w", pivotNode, err)
		}

		newNode := lNode{
			key:     key,
			address: ulid.MustNew(ulid.Now(), rand.Reader).String(),
			value:   element,
		}
		newNode.setPrev(side, otherNode.address)
		newNode.setNext(side, pivotNode.address)

		actions = append(actions, otherNode.updateSideAction(side.otherSide(), newNode.address, c.table))
		actions = append(actions, pivotNode.updateSideAction(side, newNode.address, c.table))
		actions = append(actions, newNode.putAction(c.table))
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
	}

	return newLength, true, err
}

func (c Client) listNodeAtPivot(key string, pivot string, side Side) (node lNode, found bool, err error) {
	node, found, err = c.lFindEnd(key, side)
	for found {
		if err != nil {
			return
		}

		if node.value == pivot {
			return node, true, nil
		}

		node, found, err = c.lGetByAddress(key, node.next(side))
	}

	return node, false, nil
}

func (c Client) LLEN(key string) (length int64, err error) {
	return c.HLEN(key)
}

func (c Client) LPOP(key string) (element string, ok bool, err error) {
	return c.lPop(key, Left)
}

func (c Client) lPop(key string, side Side) (element string, ok bool, err error) {
	element, transactItems, ok, err := c.lPopActions(key, side)
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

func (c Client) lPopActions(key string, side Side) (element string, actions []dynamodb.TransactWriteItem, ok bool, err error) {
	endNode, ok, err := c.lFindEnd(key, side)
	if err != nil || !ok {
		return
	}

	element = endNode.value

	penultimateNodeAddress := endNode.next(side)
	if penultimateNodeAddress != NULL {
		penultimateKeyNode := lNode{key: key, address: penultimateNodeAddress}
		penultimateKeyNode.setPrev(side, endNode.address)

		actions = append(actions, penultimateKeyNode.updateSideAction(side, NULL, c.table))
	}

	actions = append(actions, endNode.deleteAction(c.table))

	return
}

func (c Client) LPUSH(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.lPush(key, element, Left, Flags{})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) lPush(key string, element string, side Side, flags Flags) error {
	transactionItems, err := c.lPushActions(key, element, side, flags)
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

func (c Client) lPushActions(key string, element string, side Side, flags Flags) (actions []dynamodb.TransactWriteItem, err error) {
	node := lNode{
		key:   key,
		value: element,
	} // need to set address, left and right.

	currentEndNode, existingList, err := c.lFindEnd(key, side)
	if err != nil {
		return
	}

	if !existingList && flags != nil && flags.has(IfAlreadyExists) {
		return actions, nil
	}

	if existingList {
		node.address = ulid.MustNew(ulid.Now(), rand.Reader).String()
		node.setNext(side, currentEndNode.address)
		node.setPrev(side, NULL)

		actions = append(actions, currentEndNode.updateSideAction(side, node.address, c.table))
	} else {
		// start the list with a constant address - this prevents multiple calls from overwriting it
		node.address = key
		node.left = NULL
		node.right = NULL
	}

	actions = append(actions, node.putAction(c.table))

	return
}

func (c Client) lFindEnd(key string, side Side) (node lNode, found bool, err error) {
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
	node = lParseNode(resp.Items[0])

	return c.lGetByAddress(key, node.address)
}

func (c Client) lGetByAddress(key string, address string) (node lNode, found bool, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: lNode{
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
		node = lParseNode(resp.Item)
	}

	return
}

func (c Client) LPUSHX(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.lPush(key, element, Left, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []string, err error) {
	nodeMap := make(map[string]lNode)
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
			node := lParseNode(rawNode)
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

// LREM removes the first occurrence on the given side of the given element.
func (c Client) LREM(key string, side Side, element string) (newLength int64, done bool, err error) {
	var actions []dynamodb.TransactWriteItem

	outgoingNode, found, err := c.listNodeAtPivot(key, element, side)
	if err != nil || !found {
		return newLength, false, err
	}

	switch {
	case outgoingNode.isHead():
		_, done, err = c.LPOP(key)
	case outgoingNode.isTail():
		_, done, err = c.RPOP(key)
	default:
		leftKeyNode := lNode{
			key:     key,
			address: outgoingNode.left,
			right:   outgoingNode.address,
		}
		rightKeyNode := lNode{
			key:     key,
			address: outgoingNode.right,
			left:    outgoingNode.address,
		}

		actions = append(actions, leftKeyNode.updateSideAction(Right, rightKeyNode.address, c.table))
		actions = append(actions, rightKeyNode.updateSideAction(Left, leftKeyNode.address, c.table))
		actions = append(actions, outgoingNode.deleteAction(c.table))
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err == nil {
			done = true
		}
	}

	return newLength, done, err
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	node, found, err := c.lNodeAtIndex(key, index)
	if err != nil || !found {
		return
	}

	updater := newExpresionBuilder()
	updater.addConditionExists(pk)
	updater.updateSET(vk, StringValue{element})

	_, err = c.ddbClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ConditionExpression:       updater.conditionExpression(),
		ExpressionAttributeNames:  updater.expressionAttributeNames(),
		ExpressionAttributeValues: updater.expressionAttributeValues(),
		Key:                       node.keyAV(),
		TableName:                 aws.String(c.table),
		UpdateExpression:          updater.updateExpression(),
	}).Send(context.TODO())

	if err != nil {
		return
	}

	return true, nil
}

func (c Client) RPOP(key string) (element string, ok bool, err error) {
	return c.lPop(key, Right)
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element string, ok bool, err error) {
	if sourceKey == destinationKey {
		return c.lRotate(sourceKey)
	}

	element, popTransactionItems, ok, err := c.lPopActions(sourceKey, Right)
	if err != nil || !ok {
		return
	}

	pushTransactionItems, err := c.lPushActions(destinationKey, element, Left, Flags{})

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

func (c Client) lRotate(key string) (element string, ok bool, err error) {
	var actions []dynamodb.TransactWriteItem

	rightEnd, ok, err := c.lFindEnd(key, Right)

	if err != nil || !ok {
		return
	}

	leftEnd, ok, err := c.lFindEnd(key, Left)
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
		middle, ok, err := c.lGetByAddress(key, leftEnd.right)
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
		penultimateRight, ok, err := c.lGetByAddress(key, rightEnd.left)
		if err != nil {
			return element, ok, err
		}

		if !ok {
			return element, ok, errors.New("concurrent modification")
		}

		actions = append(actions, leftEnd.updateSideAction(Left, rightEnd.address, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(NULL, leftEnd.address, c.table))
		actions = append(actions, penultimateRight.updateSideAction(Right, NULL, c.table))
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
		err = c.lPush(key, element, Right, nil)
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}

func (c Client) RPUSHX(key string, elements ...string) (newLength int64, err error) {
	for _, element := range elements {
		err = c.lPush(key, element, Right, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
		newLength++
	}

	return
}
