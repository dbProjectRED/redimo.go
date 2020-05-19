package redimo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/oklog/ulid"
)

type LSide string

func (s LSide) otherSide() (otherSide LSide) {
	switch s {
	case Left:
		otherSide = Right
	case Right:
		otherSide = Left
	}

	return
}

const (
	Left  LSide = "LEFT"
	Right LSide = "RIGHT"
)
const skLeft = "left"
const skRight = "right"
const skEndMarker = skN

type listNode struct {
	key     string
	address string
	left    string
	right   string
	value   ReturnValue
}

const listNull = "NULL"
const capCount = 2

func (c Client) listCountKey(key string) keyDef {
	return keyDef{
		pk: strings.Join([]string{"_redimo", key}, "/"),
		sk: "count",
	}
}

func (ln listNode) toAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = StringValue{ln.key}.ToAV()
	avm[sk] = StringValue{ln.address}.ToAV()
	avm[skLeft] = StringValue{ln.left}.ToAV()
	avm[skRight] = StringValue{ln.right}.ToAV()
	avm[vk] = ln.value.av

	if ln.isHead() || ln.isTail() {
		avm[skEndMarker] = IntValue{1}.ToAV()
	}

	return avm
}

func (ln listNode) keyAV() map[string]dynamodb.AttributeValue {
	avm := map[string]dynamodb.AttributeValue{}
	avm[pk] = StringValue{ln.key}.ToAV()
	avm[sk] = StringValue{ln.address}.ToAV()

	return avm
}

func (ln listNode) next(side LSide) (address string) {
	switch side {
	case Left:
		address = ln.right
	case Right:
		address = ln.left
	}

	return
}

func (ln listNode) prev(side LSide) (address string) {
	switch side {
	case Left:
		address = ln.left
	case Right:
		address = ln.right
	}

	return
}

func (ln listNode) prevAttr(side LSide) (attribute string) {
	switch side {
	case Left:
		attribute = skLeft
	case Right:
		attribute = skRight
	}

	return
}

func (ln listNode) nextAttr(side LSide) (attribute string) {
	switch side {
	case Left:
		attribute = skRight
	case Right:
		attribute = skLeft
	}

	return
}

func (ln *listNode) setNext(side LSide, address string) {
	switch side {
	case Left:
		ln.right = address
	case Right:
		ln.left = address
	}
}

func (ln *listNode) setPrev(side LSide, address string) {
	switch side {
	case Left:
		ln.left = address
	case Right:
		ln.right = address
	}
}

func (ln listNode) updateBothSidesAction(newLeft string, newRight string, table string) dynamodb.TransactWriteItem {
	updater := newExpresionBuilder()
	updater.addConditionEquality(skLeft, StringValue{ln.left})
	updater.addConditionEquality(skRight, StringValue{ln.right})
	updater.updateSET(skLeft, StringValue{newLeft})
	updater.updateSET(skRight, StringValue{newRight})

	if newLeft == listNull || newRight == listNull {
		updater.updateSET(skEndMarker, IntValue{1})
	} else {
		updater.updateSET(skEndMarker, IntValue{0})
	}

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

func (ln listNode) updateSideAction(side LSide, newAddress string, table string) dynamodb.TransactWriteItem {
	updater := newExpresionBuilder()
	updater.addConditionEquality(ln.prevAttr(side), StringValue{ln.prev(side)})
	updater.addConditionEquality(ln.nextAttr(side), StringValue{ln.next(side)})
	updater.updateSET(ln.prevAttr(side), StringValue{newAddress})

	if newAddress == listNull || ln.next(side) == listNull {
		updater.updateSET(skEndMarker, IntValue{1})
	} else {
		updater.updateSET(skEndMarker, IntValue{0})
	}

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

func (ln listNode) isTail() bool {
	return ln.right == listNull
}

func (ln listNode) isHead() bool {
	return ln.left == listNull
}

func (ln listNode) putAction(table string) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      ln.toAV(),
			TableName: aws.String(table),
		},
	}
}

func (ln listNode) deleteAction(table string) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:       ln.keyAV(),
			TableName: aws.String(table),
		},
	}
}

func lParseNode(avm map[string]dynamodb.AttributeValue) (ln listNode) {
	ln.key = aws.StringValue(avm[pk].S)
	ln.address = aws.StringValue(avm[sk].S)
	ln.left = aws.StringValue(avm[skLeft].S)
	ln.right = aws.StringValue(avm[skRight].S)
	ln.value = ReturnValue{avm[vk]}

	return
}

func (c Client) LINDEX(key string, index int64) (element ReturnValue, found bool, err error) {
	node, found, err := c.listNodeAtIndex(key, index)
	if err != nil {
		return
	}

	return node.value, found, err
}

func (c Client) listNodeAtIndex(key string, index int64) (node listNode, found bool, err error) {
	side := Left
	if index < 0 {
		side = Right
		index = -index - 1
	}

	node, found, err = c.listFindEnd(key, side)
	i := int64(0)

	for found {
		if err != nil {
			return
		}

		if i == index {
			return node, true, nil
		}

		node, found, err = c.listGetByAddress(key, node.next(side))
		i++
	}

	return node, false, nil
}

// LINSERT inserts the given element on the given side of the pivot element.
func (c Client) LINSERT(key string, side LSide, pivot, element Value) (newLength int64, done bool, err error) {
	var actions []dynamodb.TransactWriteItem

	pivotNode, found, err := c.listNodeAtPivot(key, pivot, Left)
	if err != nil || !found {
		return newLength, false, err
	}

	switch {
	case pivotNode.isHead() && side == Left:
		_, err = c.LPUSHX(key, element)
		done = true
	case pivotNode.isTail() && side == Right:
		_, err = c.RPUSHX(key, element)
		done = true
	default:
		otherNode, ok, err := c.listGetByAddress(key, pivotNode.prev(side))
		if err != nil || !ok {
			return newLength, false, fmt.Errorf("could not find or load required node %v: %w", pivotNode, err)
		}

		newNode := listNode{
			key:     key,
			address: ulid.MustNew(ulid.Now(), rand.Reader).String(),
			value:   ReturnValue{element.ToAV()},
		}
		newNode.setPrev(side, otherNode.address)
		newNode.setNext(side, pivotNode.address)

		actions = append(actions, otherNode.updateSideAction(side.otherSide(), newNode.address, c.table))
		actions = append(actions, pivotNode.updateSideAction(side, newNode.address, c.table))
		actions = append(actions, newNode.putAction(c.table))
		actions = append(actions, c.listCountDeltaAction(key, 1))
	}

	if err != nil {
		return newLength, done, err
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err != nil {
			return newLength, done, err
		}

		done = true
	}

	newLength, err = c.LLEN(key)

	return newLength, done, err
}

func (c Client) listNodeAtPivot(key string, pivot Value, side LSide) (node listNode, found bool, err error) {
	node, found, err = c.listFindEnd(key, side)
	for found {
		if err != nil {
			return
		}

		if node.value.Equals(ReturnValue{pivot.ToAV()}) {
			return node, true, nil
		}

		node, found, err = c.listGetByAddress(key, node.next(side))
	}

	return node, false, nil
}

func (c Client) LLEN(key string) (length int64, err error) {
	resp, err := c.ddbClient.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key:            c.listCountKey(key).toAV(),
		TableName:      aws.String(c.table),
	}).Send(context.TODO())
	if err == nil {
		length = parseItem(resp.Item).val.Int()
	}

	return
}

func (c Client) LPOP(key string) (element ReturnValue, ok bool, err error) {
	return c.listPop(key, Left)
}

func (c Client) listPop(key string, side LSide) (element ReturnValue, ok bool, err error) {
	element, transactItems, ok, err := c.listPopActions(key, side)
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

func (c Client) listPopActions(key string, side LSide) (element ReturnValue, actions []dynamodb.TransactWriteItem, ok bool, err error) {
	endNode, ok, err := c.listFindEnd(key, side)
	if err != nil || !ok {
		return
	}

	element = endNode.value

	penultimateNodeAddress := endNode.next(side)
	if penultimateNodeAddress != listNull {
		penultimateKeyNode, found, err := c.listGetByAddress(key, penultimateNodeAddress)
		if !found || err != nil {
			return element, actions, false, err
		}

		penultimateKeyNode.setPrev(side, endNode.address)

		actions = append(actions, penultimateKeyNode.updateSideAction(side, listNull, c.table))
	}

	actions = append(actions, endNode.deleteAction(c.table))
	actions = append(actions, c.listCountDeltaAction(key, -1))

	return
}

func (c Client) LPUSH(key string, elements ...Value) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Left, Flags{})
		if err != nil {
			return newLength, err
		}
	}

	newLength, err = c.LLEN(key)

	return
}

func (c Client) listPush(key string, element Value, side LSide, flags Flags) error {
	transactionItems, err := c.listPushActions(key, element, side, flags)
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

func (c Client) listPushActions(key string, element Value, side LSide, flags Flags) (actions []dynamodb.TransactWriteItem, err error) {
	node := listNode{
		key:   key,
		value: ReturnValue{element.ToAV()},
	} // need to set address, left and right.

	currentEndNode, existingList, err := c.listFindEnd(key, side)
	if err != nil {
		return
	}

	if !existingList && flags != nil && flags.has(IfAlreadyExists) {
		return actions, nil
	}

	if existingList {
		node.address = ulid.MustNew(ulid.Now(), rand.Reader).String()
		node.setNext(side, currentEndNode.address)
		node.setPrev(side, listNull)

		actions = append(actions, currentEndNode.updateSideAction(side, node.address, c.table))
	} else {
		// start the list with a constant address - this prevents multiple calls from overwriting it
		node.address = key
		node.left = listNull
		node.right = listNull
	}

	actions = append(actions, node.putAction(c.table))

	actions = append(actions, c.listCountDeltaAction(key, 1))

	return
}

func (c Client) listCountDeltaAction(key string, delta int64) dynamodb.TransactWriteItem {
	return dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
				":delta": IntValue{delta}.ToAV(),
			},
			Key:              c.listCountKey(key).toAV(),
			TableName:        aws.String(c.table),
			UpdateExpression: aws.String(fmt.Sprintf("ADD %v :delta", vk)),
		},
	}
}

func (c Client) listFindEnd(key string, side LSide) (node listNode, found bool, err error) {
	queryCondition := newExpresionBuilder()
	queryCondition.addConditionEquality(pk, StringValue{key})
	queryCondition.addConditionEquality(skEndMarker, IntValue{1})

	resp, err := c.ddbClient.QueryRequest(&dynamodb.QueryInput{
		ConsistentRead:            aws.Bool(true),
		ExpressionAttributeNames:  queryCondition.expressionAttributeNames(),
		ExpressionAttributeValues: queryCondition.expressionAttributeValues(),
		IndexName:                 c.getIndex(skEndMarker),
		KeyConditionExpression:    queryCondition.conditionExpression(),
		Limit:                     aws.Int64(capCount),
		TableName:                 aws.String(c.table),
	}).Send(context.TODO())

	if err != nil || len(resp.Items) == 0 {
		return
	}

	for _, item := range resp.Items {
		node = lParseNode(item)
		node, found, err = c.listGetByAddress(key, node.address)

		if !found || err != nil {
			return
		}

		if node.prev(side) == listNull {
			return node, true, nil
		}
	}

	return
}

func (c Client) listGetByAddress(key string, address string) (node listNode, found bool, err error) {
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
		node = lParseNode(resp.Item)
	}

	return
}

func (c Client) LPUSHX(key string, elements ...Value) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Left, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
	}

	newLength, err = c.LLEN(key)

	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []ReturnValue, err error) {
	nodeMap := make(map[string]listNode)
	// The most common case is a full fetch, so let's start with that for now.
	queryCondition := newExpresionBuilder()
	queryCondition.addConditionEquality(pk, StringValue{key})

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

			if node.left == listNull {
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
func (c Client) LREM(key string, side LSide, element Value) (newLength int64, done bool, err error) {
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
		leftKeyNode, found, err := c.listGetByAddress(key, outgoingNode.left)
		if !found || err != nil {
			return newLength, false, err
		}

		rightKeyNode, found, err := c.listGetByAddress(key, outgoingNode.right)

		if !found || err != nil {
			return newLength, false, err
		}

		actions = append(actions, leftKeyNode.updateSideAction(Right, rightKeyNode.address, c.table))
		actions = append(actions, rightKeyNode.updateSideAction(Left, leftKeyNode.address, c.table))
		actions = append(actions, outgoingNode.deleteAction(c.table))
		actions = append(actions, c.listCountDeltaAction(key, -1))
	}

	if err != nil {
		return newLength, done, err
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
		if err != nil {
			return newLength, done, err
		}

		done = true
	}

	newLength, err = c.LLEN(key)

	return newLength, done, err
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	node, found, err := c.listNodeAtIndex(key, index)
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

func (c Client) RPOP(key string) (element ReturnValue, ok bool, err error) {
	return c.listPop(key, Right)
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element ReturnValue, ok bool, err error) {
	if sourceKey == destinationKey {
		return c.listRotate(sourceKey)
	}

	element, popTransactionItems, ok, err := c.listPopActions(sourceKey, Right)
	if err != nil || !ok {
		return
	}

	pushTransactionItems, err := c.listPushActions(destinationKey, element, Left, Flags{})

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

func (c Client) listRotate(key string) (element ReturnValue, ok bool, err error) {
	var actions []dynamodb.TransactWriteItem

	rightEnd, ok, err := c.listFindEnd(key, Right)

	if err != nil || !ok {
		return
	}

	leftEnd, ok, err := c.listFindEnd(key, Left)
	if err != nil || !ok {
		return
	}

	switch {
	case rightEnd.address == leftEnd.address:
		element = rightEnd.value
		// no action to take

	case leftEnd.right == rightEnd.address:
		actions = append(actions, leftEnd.updateBothSidesAction(rightEnd.address, listNull, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(listNull, leftEnd.address, c.table))
		element = rightEnd.value

	case leftEnd.right == rightEnd.left:
		middle, ok, err := c.listGetByAddress(key, leftEnd.right)
		if err != nil {
			return element, ok, err
		}

		if !ok {
			return element, ok, errors.New("concurrent modification")
		}

		actions = append(actions, leftEnd.updateBothSidesAction(rightEnd.address, middle.address, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(listNull, leftEnd.address, c.table))
		actions = append(actions, middle.updateBothSidesAction(leftEnd.address, listNull, c.table))
		element = rightEnd.value

	default:
		penultimateRight, ok, err := c.listGetByAddress(key, rightEnd.left)
		if err != nil {
			return element, ok, err
		}

		if !ok {
			return element, ok, errors.New("concurrent modification")
		}

		actions = append(actions, leftEnd.updateSideAction(Left, rightEnd.address, c.table))
		actions = append(actions, rightEnd.updateBothSidesAction(listNull, leftEnd.address, c.table))
		actions = append(actions, penultimateRight.updateSideAction(Right, listNull, c.table))
		element = rightEnd.value
	}

	if len(actions) > 0 {
		_, err = c.ddbClient.TransactWriteItemsRequest(&dynamodb.TransactWriteItemsInput{
			TransactItems: actions,
		}).Send(context.TODO())
	}

	return
}

func (c Client) RPUSH(key string, elements ...Value) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Right, nil)
		if err != nil {
			return newLength, err
		}
	}

	newLength, err = c.LLEN(key)

	return
}

func (c Client) RPUSHX(key string, elements ...Value) (newLength int64, err error) {
	for _, element := range elements {
		err = c.listPush(key, element, Right, Flags{IfAlreadyExists})
		if err != nil {
			return newLength, err
		}
	}

	newLength, err = c.LLEN(key)

	return
}
