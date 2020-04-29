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

func newClient(t *testing.T) Client {
	t.Parallel()

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

	return Client{
		ddbClient:       dynamoService,
		consistentReads: true,
		table:           name,
	}
}

func newConfig(t *testing.T) aws.Config {
	cfgs := external.Configs{}
	cfgs, err := cfgs.AppendFromLoaders(external.DefaultConfigLoaders)
	assert.NoError(t, err)
	cfg, err := cfgs.ResolveAWSConfig([]external.AWSConfigResolver{
		external.ResolveDefaultAWSConfig,
		//external.ResolveHandlersFunc,
		//external.ResolveEndpointResolverFunc,
		//external.ResolveCustomCABundle,
		//external.ResolveEnableEndpointDiscovery,
		//
		//external.ResolveRegion,
		//external.ResolveEC2Region,
		//external.ResolveDefaultRegion,
		//
		//external.ResolveCredentials,
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
