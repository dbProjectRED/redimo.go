package redimo

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestClientBuilder(t *testing.T) {
	dynamoService := dynamodb.New(newConfig(t))
	c1 := NewClient(dynamoService)
	assert.Equal(t, c1.ddbClient, dynamoService)
	assert.True(t, c1.consistentReads)
	assert.Equal(t, "redimo", c1.table)
	assert.Equal(t, c1.pk, c1.pk)
	assert.False(t, c1.EventuallyConsistent().consistentReads)
	c2 := c1.Table("table2", "index2").EventuallyConsistent()
	assert.Equal(t, "table2", c2.table)
	assert.Equal(t, "index2", c2.index)
	assert.False(t, c2.consistentReads)
	assert.True(t, c1.consistentReads)
	assert.True(t, c2.StronglyConsistent().consistentReads)
}

func newClient(t *testing.T) Client {
	t.Parallel()

	tableName := uuid.New().String()
	indexName := "idx"
	partitionKey := "pk"
	sortKey := "sk"
	sortKeyNum := "skN"
	dynamoService := dynamodb.New(newConfig(t))
	_, err := dynamoService.CreateTableRequest(&dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: aws.String(partitionKey), AttributeType: "S"},
			{AttributeName: aws.String(sortKey), AttributeType: "S"},
			{AttributeName: aws.String(sortKeyNum), AttributeType: "N"},
		},
		BillingMode:            "",
		GlobalSecondaryIndexes: nil,
		KeySchema: []dynamodb.KeySchemaElement{
			{AttributeName: aws.String(partitionKey), KeyType: dynamodb.KeyTypeHash},
			{AttributeName: aws.String(sortKey), KeyType: dynamodb.KeyTypeRange},
		},
		LocalSecondaryIndexes: []dynamodb.LocalSecondaryIndex{
			{
				IndexName: aws.String(indexName),
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: aws.String(partitionKey), KeyType: dynamodb.KeyTypeHash},
					{AttributeName: aws.String(sortKeyNum), KeyType: dynamodb.KeyTypeRange},
				},
				Projection: &dynamodb.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   dynamodb.ProjectionTypeKeysOnly,
				},
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		SSESpecification:    nil,
		StreamSpecification: nil,
		TableName:           aws.String(tableName),
		Tags:                nil,
	}).Send(context.TODO())
	assert.NoError(t, err)

	return NewClient(dynamoService).Table(tableName, indexName).Attributes(partitionKey, sortKey, sortKeyNum)
}

func newConfig(t *testing.T) aws.Config {
	cfgs := external.Configs{}
	cfgs, err := cfgs.AppendFromLoaders(external.DefaultConfigLoaders)
	assert.NoError(t, err)
	cfg, err := cfgs.ResolveAWSConfig([]external.AWSConfigResolver{
		external.ResolveDefaultAWSConfig,
		// external.ResolveHandlersFunc,
		// external.ResolveEndpointResolverFunc,
		// external.ResolveCustomCABundle,
		// external.ResolveEnableEndpointDiscovery,
		//
		// external.ResolveRegion,
		// external.ResolveEC2Region,
		// external.ResolveDefaultRegion,
		//
		// external.ResolveCredentials,
	})
	assert.NoError(t, err)

	cfg.Credentials = aws.NewStaticCredentialsProvider("ABCD", "EFGH", "IKJGL")
	cfg.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:8000")
	cfg.Region = "ap-south-1"
	cfg.DisableEndpointHostPrefix = true
	cfg.LogLevel = aws.LogOff
	cfg.Logger = t
	cfg.HTTPClient = http.DefaultClient

	return cfg
}
