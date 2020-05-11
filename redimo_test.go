package redimo

import (
	"context"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"strings"
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
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: "S"},
			{AttributeName: aws.String("sk"), AttributeType: "S"},
			{AttributeName: aws.String("sk2"), AttributeType: "S"},
			{AttributeName: aws.String("sk3"), AttributeType: "S"},
			{AttributeName: aws.String("sk4"), AttributeType: "S"},
			{AttributeName: aws.String("skN1"), AttributeType: "N"},
		},
		BillingMode:            "",
		GlobalSecondaryIndexes: nil,
		KeySchema: []dynamodb.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodb.KeyTypeRange},
		},
		LocalSecondaryIndexes: []dynamodb.LocalSecondaryIndex{
			{
				IndexName: aws.String("lsi_sk2"),
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
					{AttributeName: aws.String("sk2"), KeyType: dynamodb.KeyTypeRange},
				},
				Projection: &dynamodb.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   dynamodb.ProjectionTypeKeysOnly,
				},
			},
			{
				IndexName: aws.String("lsi_sk3"),
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
					{AttributeName: aws.String("sk3"), KeyType: dynamodb.KeyTypeRange},
				},
				Projection: &dynamodb.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   dynamodb.ProjectionTypeKeysOnly,
				},
			},
			{
				IndexName: aws.String("lsi_sk4"),
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
					{AttributeName: aws.String("sk4"), KeyType: dynamodb.KeyTypeRange},
				},
				Projection: &dynamodb.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   dynamodb.ProjectionTypeKeysOnly,
				},
			},
			{
				IndexName: aws.String("lsi_skN1"),
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodb.KeyTypeHash},
					{AttributeName: aws.String("skN1"), KeyType: dynamodb.KeyTypeRange},
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
		TableName:           aws.String(name),
		Tags:                nil,
	}).Send(context.TODO())
	assert.NoError(t, err)

	return Client{
		ddbClient:       dynamoService,
		consistentReads: true,
		table:           name,
		indexes: map[string]string{
			sk2: "lsi_sk2",
			sk3: "lsi_sk3",
			sk4: "lsi_sk4",
		},
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

func TestFloatLexConversions(t *testing.T) {
	t.Parallel()

	for i := 0; i < 100; i++ {
		checkFloat(t, big.NewFloat(rand.Float64()))
		checkFloat(t, big.NewFloat(rand.ExpFloat64()))
		checkFloat(t, big.NewFloat(rand.NormFloat64()))
	}

	checkFloat(t, big.NewFloat(1.92384792872902345023085473838472938e-6))
	checkFloat(t, big.NewFloat(2838.398672948723987e2))
	checkFloat(t, big.NewFloat(-2838.398672948723987))
	checkFloat(t, big.NewFloat(-2838.398672948723987e-5))
	checkFloat(t, big.NewFloat(math.MaxFloat64))
	checkFloat(t, big.NewFloat(math.MaxFloat32))
	checkFloat(t, big.NewFloat(math.SmallestNonzeroFloat64))
	checkFloat(t, big.NewFloat(math.SmallestNonzeroFloat32))
	checkFloat(t, big.NewFloat(0))
	checkFloat(t, big.NewFloat(math.Inf(-1)))
	checkFloat(t, big.NewFloat(math.Inf(+1)))
}

func checkFloat(t *testing.T, rfloat *big.Float) {
	lex := floatToLex(rfloat)
	parsed := lexToFloat(lex + "anything else")
	assert.Equal(t, 22, len(lex))

	expected, _ := rfloat.Float64()
	actual, _ := parsed.Float64()
	assert.Equal(t, expected, actual, lex)

	comparison := big.NewFloat(rand.NormFloat64())
	assert.Equal(t, comparison.Cmp(rfloat), strings.Compare(floatToLex(comparison), lex))
}
