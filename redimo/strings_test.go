package redimo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasic(t *testing.T) {
	rc := newRedimoClient(t)
	val, ok, err := rc.GET("hello")
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = rc.SET("hello", StringValue{"world"}, nil, Flags{})
	assert.NoError(t, err)
	assert.True(t, ok)

	val, ok, err = rc.GET("hello")
	assert.NoError(t, err)
	assert.True(t, ok)

	str, ok := val.AsString()
	assert.True(t, ok)
	assert.Equal(t, "world", str)
}

func newRedimoClient(t *testing.T) RedimoClient {
	name := uuid.New().String()
	dynamoService := dynamodb.New(newConfig(t))
	_, err := dynamoService.CreateTableRequest(&dynamodb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: "S"},
			{AttributeName: aws.String("sk"), AttributeType: "S"},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}).Send(context.TODO())
	assert.NoError(t, err)
	return RedimoClient{
		client:            dynamoService,
		strongConsistency: true,
		table:             name,
	}
}

func newConfig(t *testing.T) aws.Config {
	cfg, err := external.LoadDefaultAWSConfig()
	assert.NoError(t, err)
	cfg.Credentials = aws.NewStaticCredentialsProvider("ABCD", "EFGH", "IKJGL")
	cfg.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:8000")
	cfg.Region = "ap-south-1"
	cfg.DisableEndpointHostPrefix = true
	cfg.LogLevel = aws.LogOff
	cfg.Logger = t
	return cfg
}
