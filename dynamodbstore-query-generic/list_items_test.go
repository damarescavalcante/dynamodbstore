package dynamodbstore

import (
    "context"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

// Mock data structures for each type
type Bundle struct {
    ID   string
    Name string
}

type CAJournal struct {
    ID        string
    Timestamp time.Time
}

type Entry struct {
    SpiffeID string
    ParentID string
}

type EntryEvent struct {
    EventID   int
    CreatedAt time.Time
}

type FederationRelationship struct {
    TrustDomain string
    BundleURL   string
}

type JoinToken struct {
    Token     string
    ExpiresAt time.Time
}

type NodeEvent struct {
    NodeID    string
    Timestamp time.Time
}

// MockDynamoDBClient simulates the DynamoDB client
type MockDynamoDBClient struct {
    mock.Mock
}

func (m *MockDynamoDBClient) Query(ctx context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
    args := m.Called(ctx, input)
    return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

// Configures and runs a generic test with a specific type
func setupGenericTest[T any](mockClient *MockDynamoDBClient, items []map[string]types.AttributeValue, t *testing.T, projection []string, tableName, partitionKey string) []T {
    ctx := context.Background()

    // Configure the mock to return simulated items
    mockClient.On("Query", ctx, mock.Anything).Return(&dynamodb.QueryOutput{
        Items: items,
    }, nil)

    filters := []Filter{{Name: partitionKey, Op: EqualTo, Value: "test-partition-key"}}
    pagination := &Pagination{Limit: 10}

    results, _, err := ListItems[T](ctx, tableName, mockClient, partitionKey, filters, pagination, projection)
    assert.NoError(t, err)
    assert.NotNil(t, results)

    mockClient.AssertExpectations(t)
    return results
}

// Specific tests for each type
func TestListBundles(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"ID": &types.AttributeValueMemberS{Value: "bundle1"}, "Name": &types.AttributeValueMemberS{Value: "Bundle One"}},
    }
    projection := []string{"ID", "Name"}
    results := setupGenericTest[Bundle](mockClient, items, t, projection, "BundlesTable", "ID")

    assert.Equal(t, "bundle1", results[0].ID)
    assert.Equal(t, "Bundle One", results[0].Name)
}

func TestListCAJournals(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"ID": &types.AttributeValueMemberS{Value: "journal1"}, "Timestamp": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"ID", "Timestamp"}
    results := setupGenericTest[CAJournal](mockClient, items, t, projection, "CAJournalsTable", "ID")

    assert.Equal(t, "journal1", results[0].ID)
    assert.NotZero(t, results[0].Timestamp)
}

func TestListEntries(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"SpiffeID": &types.AttributeValueMemberS{Value: "spiffe://example.org/node"}, "ParentID": &types.AttributeValueMemberS{Value: "spiffe://example.org/parent"}},
    }
    projection := []string{"SpiffeID", "ParentID"}
    results := setupGenericTest[Entry](mockClient, items, t, projection, "EntriesTable", "SpiffeID")

    assert.Equal(t, "spiffe://example.org/node", results[0].SpiffeID)
    assert.Equal(t, "spiffe://example.org/parent", results[0].ParentID)
}

func TestListEntryEvents(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"EventID": &types.AttributeValueMemberN{Value: "1"}, "CreatedAt": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"EventID", "CreatedAt"}
    results := setupGenericTest[EntryEvent](mockClient, items, t, projection, "EntryEventsTable", "EventID")

    assert.Equal(t, 1, results[0].EventID)
    assert.NotZero(t, results[0].CreatedAt)
}

func TestListFederationRelationships(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"TrustDomain": &types.AttributeValueMemberS{Value: "example.org"}, "BundleURL": &types.AttributeValueMemberS{Value: "https://example.org/bundle"}},
    }
    projection := []string{"TrustDomain", "BundleURL"}
    results := setupGenericTest[FederationRelationship](mockClient, items, t, projection, "FederationRelationshipsTable", "TrustDomain")

    assert.Equal(t, "example.org", results[0].TrustDomain)
    assert.Equal(t, "https://example.org/bundle", results[0].BundleURL)
}

func TestListJoinTokens(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"Token": &types.AttributeValueMemberS{Value: "token123"}, "ExpiresAt": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"Token", "ExpiresAt"}
    results := setupGenericTest[JoinToken](mockClient, items, t, projection, "JoinTokensTable", "Token")

    assert.Equal(t, "token123", results[0].Token)
    assert.NotZero(t, results[0].ExpiresAt)
}

func TestListNodeEvents(t *testing.T) {
    mockClient := new(MockDynamoDBClient)
    items := []map[string]types.AttributeValue{
        {"NodeID": &types.AttributeValueMemberS{Value: "node1"}, "Timestamp": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"}},
    }
    projection := []string{"NodeID", "Timestamp"}
    results := setupGenericTest[NodeEvent](mockClient, items, t, projection, "NodeEventsTable", "NodeID")

    assert.Equal(t, "node1", results[0].NodeID)
    assert.NotZero(t, results[0].Timestamp)
}
