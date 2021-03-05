package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	filename   = "C:\\borderfree\\multiTimeline.csv"
	Table_Name = "CupCakes"
	REGION     = "us-east-2"
)

type Schema struct {
	Id       int
	Month    string
	Interest int
}

var dynamo *dynamodb.DynamoDB

func init() {
	dynamo = connectDynamo()
}

func connectDynamo() (db *dynamodb.DynamoDB) {
	return dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(REGION),
		Credentials: credentials.NewStaticCredentials(os.Getenv("ACCESS_KEY_"), os.Getenv("SECRET_KEY_"), ""),,
	})))
}

func main() {
	importDataToDB()
}

func importDataToDB() {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("An error occurred ", err)
		return
	}

	fileReader := csv.NewReader(file)
	fileReader.FieldsPerRecord = -1

	headers, err := fileReader.Read()
	if err != nil {
		fmt.Println("An error occurred ", err)
		return
	}

	fmt.Println(headers)

	headers1, err := fileReader.Read()
	if err != nil {
		fmt.Println("An error occurred ", err)
		return
	}

	fmt.Println(headers1)
// 	iterating throught the data set and adding the items to dynamodb

	for i := 0; ; i = i + 1 {
		records, err := fileReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("An error occurred ", err)
			return
		}

		value, err := strconv.Atoi(records[1])

		var schema Schema = Schema{
			Id:       i + 1,
			Month:    records[0],
			Interest: value,
		}

		printSchema(schema)
		insertItem(schema)
	}

}

// insert method
func insertItem(schema Schema) {
	_, err := dynamo.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"Id": {
				N: aws.String(strconv.Itoa(schema.Id)),
			},
			"Month": {
				S: aws.String(schema.Month),
			},
			"Interest": {
				N: aws.String(strconv.Itoa(schema.Interest)),
			},
		},
		TableName: aws.String(Table_Name),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println(aerr.Error())
		}
	}
}

func printSchema(schema Schema) {
	fmt.Println(schema.Id)
	fmt.Println(schema.Month)
	fmt.Println(schema.Interest)
}
