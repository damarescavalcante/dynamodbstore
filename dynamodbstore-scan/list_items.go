package dynamodbstore

import (
    "context"
    "fmt"

    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

// Definições de MatchBehavior para comportamentos de filtro
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

// Estrutura de filtro para as operações de listagem
type Filter struct {
    Name  string
    Op    MatchBehavior
    Value interface{}
}

// Estrutura de paginação para controle dos resultados
type Pagination struct {
    Token     string // Token para a próxima página
    Limit     int    // Limite de itens por página
    NextToken string // Token atualizado após a consulta
}

// Interface para simular o cliente DynamoDB
type DynamoDBAPI interface {
    Scan(ctx context.Context, input *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Função genérica de listagem para DynamoDB
func ListItems[T any](ctx context.Context, tableName string, dynamoClient DynamoDBAPI, filters []Filter, pagination *Pagination, projection []string) ([]T, *Pagination, error) {
    var filterExpression expression.ConditionBuilder
    hasFilters := false

    for _, filter := range filters {
        field := expression.Name(filter.Name)

        // Define a expressão de acordo com o tipo de operação
        switch filter.Op {
        case EqualTo:
            filterExpression = field.Equal(expression.Value(filter.Value))
        case LessThan:
            filterExpression = field.LessThan(expression.Value(filter.Value))
        case GreaterThan:
            filterExpression = field.GreaterThan(expression.Value(filter.Value))
        case MatchAny:
            filterExpression = expression.Contains(field, filter.Value)
        case MatchExact:
            filterExpression = field.Equal(expression.Value(filter.Value))
        case MatchSuperset:
            filterExpression = expression.Contains(field, expression.Value(filter.Value))
        case MatchSubset:
            filterExpression = expression.Contains(field, expression.Value(filter.Value))
        }
        hasFilters = true
    }

    // Configuração da projeção de atributos

    // Construindo a projeção de forma incremental
    builder := expression.NewBuilder()
    if len(projection) > 0 {
        // Inicia a projeção com o primeiro campo
        projBuilder := expression.NamesList(expression.Name(projection[0]))
        for _, attr := range projection[1:] {
            // Adiciona os outros campos ao projBuilder
            projBuilder = projBuilder.AddNames(expression.Name(attr))
        }
        builder = builder.WithProjection(projBuilder)
    }

    // Adicione o filtro apenas se houver filtros definidos
    if hasFilters {
        builder = builder.WithFilter(filterExpression)
    }

    expr, err := builder.Build()
    if err != nil {
        return nil, nil, fmt.Errorf("erro ao construir expressão: %w", err)
    }

    input := &dynamodb.ScanInput{
        TableName:                 &tableName,
        ExpressionAttributeNames:  expr.Names(),
        ExpressionAttributeValues: expr.Values(),
        ProjectionExpression:      expr.Projection(),
    }

    if hasFilters {
        input.FilterExpression = expr.Filter()
    }

    // Configuração de paginação
    if pagination != nil && pagination.Token != "" {
        input.ExclusiveStartKey = map[string]types.AttributeValue{
            "Key": &types.AttributeValueMemberS{Value: pagination.Token},
        }
    }

    if pagination != nil && pagination.Limit > 0 {
        limit := int32(pagination.Limit)
        input.Limit = &limit
    }

    var results []T
    scanPaginator := dynamodb.NewScanPaginator(dynamoClient, input)
    for scanPaginator.HasMorePages() {
        page, err := scanPaginator.NextPage(ctx)
        if err != nil {
            return nil, nil, fmt.Errorf("falha ao buscar registros: %w", err)
        }

        var pageResults []T
        if err := attributevalue.UnmarshalListOfMaps(page.Items, &pageResults); err != nil {
            return nil, nil, fmt.Errorf("falha ao deserializar registros: %w", err)
        }

        results = append(results, pageResults...)

        if pagination != nil && page.LastEvaluatedKey != nil {
            pagination.NextToken = page.LastEvaluatedKey["Key"].(*types.AttributeValueMemberS).Value
            break
        }
    }

    return results, pagination, nil
}
