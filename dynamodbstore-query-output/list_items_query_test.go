package dynamodbstore

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

type MockDynamoDBClient struct {
    mock.Mock
}

func (m *MockDynamoDBClient) Query(ctx context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
    args := m.Called(ctx, input)
    return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

func setupQueryTest(mockClient *MockDynamoDBClient, items []map[string]types.AttributeValue, t *testing.T, projection []string, tableName, partitionKey string) *dynamodb.QueryOutput {
    ctx := context.Background()

    mockClient.On("Query", ctx, mock.Anything).Return(&dynamodb.QueryOutput{
        Items: items,
    }, nil)

    filters := []Filter{{Name: partitionKey, Op: EqualTo, Value: "test-partition-key"}}
    pagination := &Pagination{Limit: 10}

    output, err := ListItems(ctx, tableName, mockClient, partitionKey, filters, pagination, projection)
    assert.NoError(t, err)
    assert.NotNil(t, output)

    mockClient.AssertExpectations(t)
    return output
}

func TestListBundlesQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"ID": &types.AttributeValueMemberS{Value: "bundle1"}, "Name": &types.AttributeValueMemberS{Value: "Bundle One"}},
    }
    projection := []string{"ID", "Name"}
    output := setupQueryTest(mockClient, items, t, projection, "BundlesTable", "ID")

    assert.Equal(t, items, output.Items)
}

func TestListCAJournalsQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"ID": &types.AttributeValueMemberS{Value: "journal1"}, "Timestamp": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"ID", "Timestamp"}
    output := setupQueryTest(mockClient, items, t, projection, "CAJournalsTable", "ID")

    assert.Equal(t, items, output.Items)
}

func TestListEntriesQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"SpiffeID": &types.AttributeValueMemberS{Value: "spiffe://example.org/node"}, "ParentID": &types.AttributeValueMemberS{Value: "spiffe://example.org/parent"}},
    }
    projection := []string{"SpiffeID", "ParentID"}
    output := setupQueryTest(mockClient, items, t, projection, "EntriesTable", "SpiffeID")

    assert.Equal(t, items, output.Items)
}

func TestListEntryEventsQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"EventID": &types.AttributeValueMemberN{Value: "1"}, "CreatedAt": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"EventID", "CreatedAt"}
    output := setupQueryTest(mockClient, items, t, projection, "EntryEventsTable", "EventID")

    assert.Equal(t, items, output.Items)
}

func TestListFederationRelationshipsQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"TrustDomain": &types.AttributeValueMemberS{Value: "example.org"}, "BundleURL": &types.AttributeValueMemberS{Value: "https://example.org/bundle"}},
    }
    projection := []string{"TrustDomain", "BundleURL"}
    output := setupQueryTest(mockClient, items, t, projection, "FederationRelationshipsTable", "TrustDomain")

    assert.Equal(t, items, output.Items)
}

func TestListJoinTokensQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"Token": &types.AttributeValueMemberS{Value: "token123"}, "ExpiresAt": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"Token", "ExpiresAt"}
    output := setupQueryTest(mockClient, items, t, projection, "JoinTokensTable", "Token")

    assert.Equal(t, items, output.Items)
}

func TestListNodeEventsQuery(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"NodeID": &types.AttributeValueMemberS{Value: "node1"}, "Timestamp": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"NodeID", "Timestamp"}
    output := setupQueryTest(mockClient, items, t, projection, "NodeEventsTable", "NodeID")

    assert.Equal(t, items, output.Items)
}
