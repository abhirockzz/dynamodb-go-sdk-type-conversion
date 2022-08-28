package db

import (
	"context"
	"dynamodb-demo-app/model"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DB struct {
	client *dynamodb.Client
	table  string
}

func New(t string) DB {
	cfg, _ := config.LoadDefaultConfig(context.Background())
	c := dynamodb.NewFromConfig(cfg)

	return DB{client: c, table: t}
}

func (d DB) Save(user model.User) error {

	item, err := attributevalue.MarshalMap(user)

	if err != nil {
		log.Println("marshal failed with error", err)
		return err
	}

	_, err = d.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item:      item})

	if err != nil {
		log.Println("dynamodb put item failed")
		return err
	}

	return nil
}

var ErrNotFound = errors.New("does not exist")

func (d DB) GetOne(email, city string) (model.User, error) {

	result, err := d.client.GetItem(context.Background(),
		&dynamodb.GetItemInput{
			TableName: aws.String(d.table),
			Key: map[string]types.AttributeValue{
				"email": &types.AttributeValueMemberS{Value: email},
				"city":  &types.AttributeValueMemberS{Value: city}},
		})

	if err != nil {
		log.Println("getitem failed with error", err)
		return model.User{}, err
	}

	if result.Item == nil {
		return model.User{}, ErrNotFound
	}

	var user model.User

	err = attributevalue.UnmarshalMap(result.Item, &user)
	if err != nil {
		log.Println("unmarshal failed with error", err)
		return model.User{}, err
	}

	return user, nil
}

func (d DB) GetMany(city string) ([]model.User, error) {

	kcb := expression.Key("city").Equal(expression.Value(city))
	kce, _ := expression.NewBuilder().WithKeyCondition(kcb).Build()

	result, err := d.client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(d.table),
		KeyConditionExpression:    kce.KeyCondition(),
		ExpressionAttributeNames:  kce.Names(),
		ExpressionAttributeValues: kce.Values(),
	})

	if err != nil {
		log.Println("Query failed with error", err)
		return []model.User{}, err
	}

	users := []model.User{}

	if len(result.Items) == 0 {
		return users, nil
	}

	err = attributevalue.UnmarshalListOfMaps(result.Items, &users)
	if err != nil {
		log.Println("UnmarshalMap failed with error", err)
		return []model.User{}, err
	}

	return users, nil
}

func (d DB) GetAll() ([]model.User, error) {

	result, err := d.client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String(d.table),
	})

	if err != nil {
		log.Println("Scan failed with error", err)
		return []model.User{}, err
	}

	users := []model.User{}

	err = attributevalue.UnmarshalListOfMaps(result.Items, &users)

	if err != nil {
		log.Println("UnmarshalMap failed with error", err)
		return []model.User{}, err
	}

	return users, nil
}

func (d DB) BatchImport(total int) {
	log.Println("seeding", total, "records as test data")

	startTime := time.Now()

	cities := []string{"New Delhi", "New York", "Tel Aviv", "London", "Shanghai"}

	batchSize := 25
	processed := total

	for num := 1; num <= total; num = num + batchSize {

		batch := make(map[string][]types.WriteRequest)
		var requests []types.WriteRequest

		start := num
		end := num + 24

		for i := start; i <= end; i++ {
			user := model.User{Email: "user" + strconv.Itoa(i) + "@foo.com", City: cities[rand.Intn(len(cities))]}
			// age might be missing
			if rand.Intn(2) == 1 {
				user.Age = rand.Intn(35) + 18
			}
			item, _ := attributevalue.MarshalMap(user)
			requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		}

		batch[d.table] = requests

		op, err := d.client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})

		if err != nil {
			log.Fatal("batch write error", err)
		}

		if len(op.UnprocessedItems) != 0 {
			processed = processed - len(op.UnprocessedItems)
		}
	}

	log.Println("seed data complete. inserted", processed, "records in", time.Since(startTime).Seconds(), "seconds")

	if processed != total {
		log.Println("there were", (total - processed), "unprocessed records")
	}
}
