package redimo

import (
	"context"
	"fmt"
	"math/big"
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

func Test_fToLex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in  float64
		out string
	}{
		{in: 3.25e5, out: "5 005 3.2500000000000000"},
		{in: 8.4e-5, out: "4 994 8.4000000000000000"},
		{in: 8.4e-7, out: "4 992 8.4000000000000000"},
		{in: 7.23e-7, out: "4 992 7.2300000000000000"},
		{in: 7.23e-302, out: "4 697 7.2300000000000000"},
		{in: 0.0e0, out: "3 000 0.0000000000000000"},
		{in: -4.25e-4, out: "2 004 5.7500000000000000"},
		{in: -6.32e-4, out: "2 004 3.6800000000000000"},
		{in: -6.34e-3, out: "2 003 3.6600000000000000"},
		{in: -4.0e104, out: "1 895 6.0000000000000000"},
		{in: -4.0e105, out: "1 894 6.0000000000000000"},
		{in: -6.0e105, out: "1 894 4.0000000000000000"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%e", tt.in), func(t *testing.T) {
			assert.Equal(t, tt.out, floatToLex(tt.in), tt)

			expected := big.NewFloat(tt.in)
			expectedBigMant := new(big.Float)
			expectedExp := expected.MantExp(expectedBigMant)
			expectedMant, _ := expectedBigMant.Float64()

			actual := big.NewFloat(lexToFloat(tt.out))
			actualBigMant := new(big.Float)
			actualExp := actual.MantExp(actualBigMant)
			actualMant, _ := actualBigMant.Float64()
			assert.InDelta(t, expectedMant, actualMant, 0.001)
			assert.Equal(t, expectedExp, actualExp)

		})
	}
}
