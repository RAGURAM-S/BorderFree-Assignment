package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Schema struct {
	Id          int
	Interest    int
	update_time string
}

const (
	Table_Name = "CupCakes"
	REGION     = "us-east-2"
)

var dynamo *dynamodb.DynamoDB

func init() {
	dynamo = connectDynamo()
}

func connectDynamo() (db *dynamodb.DynamoDB) {
	return dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(REGION),
		Credentials: credentials.NewStaticCredentials(os.Getenv("ACCESS_KEY_"), os.Getenv("SECRET_KEY_"), ""),
	})))
}

func main() {
	lambda.Start(randomRecords)
}

func getCurrentTime() string {
	date := time.Now()
	value := date.Format(time.RFC850)
	return value
}

// generates an array of random non-repeating numbers
func randomNumberGenerator(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}

	nums := make([]int, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for len(nums) < count {
		value := end - start
		num := r.Intn((value)) + start
		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}
	return nums
}

// updates the interest value and update_time fields for the random-numbers generated in the randomNumberGenerator method
func randomRecords() {
	result := randomNumberGenerator(1, 206, 100)
	for i := 0; i < len(result); i++ {
		var record Schema = Schema{
			Id:          result[i],
			Interest:    rand.Intn(101),
			update_time: getCurrentTime(),
		}
		updateRecords(record)
		fmt.Println("records updated successfully!!!")
	}
}

// update method in dynamodb
func updateRecords(schema Schema) {
	_, err := dynamo.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#I": aws.String("Interest"),
			"#T": aws.String("update_time"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":Interest": {
				N: aws.String(strconv.Itoa(schema.Interest)),
			},
			":update_time": {
				S: aws.String(schema.update_time),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"Id": {
				N: aws.String(strconv.Itoa(schema.Id)),
			},
		},
		TableName:        aws.String(Table_Name),
		UpdateExpression: aws.String("SET #I = :Interest, #T = :update_time"),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println(aerr.Error())
		}
	}
}
