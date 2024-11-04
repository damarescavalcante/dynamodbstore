package dynamodbstore

import (
    "context"
    "fmt"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type dynamoQueryClient interface {
    Query(ctx context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type Filter struct {
    Name  string
    Op    MatchBehavior
    Value interface{}
}

type MatchBehavior int

const (
    MatchAny MatchBehavior = iota + 1
    MatchExact
    MatchSuperset
    MatchSubset
    LessThan
    GreaterThan
    EqualTo
)

type Pagination struct {
    Token    string
    Limit    int
    NextToken string
}

func ListItems(
    ctx context.Context,
    kind string,
    dynamoClient dynamoQueryClient, 
    partitionKey string,
    filters []Filter,
    pagination *Pagination,
    projection []string,
) (*dynamodb.QueryOutput, error) {

    var keyCondition expression.KeyConditionBuilder
    var filterExpression expression.ConditionBuilder
    hasFilters := false

    for _, filter := range filters {
        field := expression.Name(filter.Name)
        
        if filter.Name == partitionKey && filter.Op == EqualTo {
            keyCondition = expression.Key(filter.Name).Equal(expression.Value(filter.Value))
            continue
        }
        
        switch filter.Op {
        case EqualTo:
            filterExpression = filterExpression.And(field.Equal(expression.Value(filter.Value)))
        case LessThan:
            filterExpression = filterExpression.And(field.LessThan(expression.Value(filter.Value)))
        case GreaterThan:
            filterExpression = filterExpression.And(field.GreaterThan(expression.Value(filter.Value)))
        case MatchAny:
            filterExpression = filterExpression.And(expression.Contains(field, expression.Value(filter.Value)))
        case MatchExact:
            filterExpression = filterExpression.And(field.Equal(expression.Value(filter.Value)))
        case MatchSuperset:
            filterExpression = filterExpression.And(expression.Contains(field, expression.Value(filter.Value)))
        case MatchSubset:
            filterExpression = filterExpression.And(expression.Contains(field, expression.Value(filter.Value)))
        }
        hasFilters = true
    }

    builder := expression.NewBuilder().WithKeyCondition(keyCondition)
    if len(projection) > 0 {
        projBuilder := expression.NamesList(expression.Name(projection[0]))
        for _, attr := range projection[1:] {
            projBuilder = projBuilder.AddNames(expression.Name(attr))
        }
        builder = builder.WithProjection(projBuilder)
    }

    if hasFilters {
        builder = builder.WithFilter(filterExpression)
    }

    expr, err := builder.Build()
    if err != nil {
        return nil, fmt.Errorf("error to building expression: %w", err)
    }

    input := &dynamodb.QueryInput{
        TableName:                 aws.String(kind),
        ExpressionAttributeNames:  expr.Names(),
        ExpressionAttributeValues: expr.Values(),
        KeyConditionExpression:    expr.KeyCondition(),
        ProjectionExpression:      expr.Projection(),
    }

    if hasFilters {
        input.FilterExpression = expr.Filter()
    }

    if pagination != nil && pagination.Token != "" {
        input.ExclusiveStartKey = map[string]types.AttributeValue{
            partitionKey: &types.AttributeValueMemberS{Value: pagination.Token},
        }
    }

    if pagination != nil && pagination.Limit > 0 {
        limit := int32(pagination.Limit)
        input.Limit = &limit
    }

    return dynamoClient.Query(ctx, input)
}
