package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

type Schema struct {
	Id       int
	Month    string
	Interest int
	// update_time string
}

func handlerMethod(ctx context.Context, event events.DynamoDBEvent) {
	// Basic information for the Amazon Elasticsearch Service domain
	domain := "https://search-cupcakes-es-xq66paz5v4dyvx5aub3s2zpvve.us-east-2.es.amazonaws.com"
	index := "updates-cupcakes"
	region := "us-east-2"
	service := "es"

	credentials := credentials.NewStaticCredentials(os.Getenv("ACCESS_KEY_"), os.Getenv("SECRET_KEY_"), "")
	signer := v4.NewSigner(credentials)

	for _, record := range event.Records {

		// Print new values for attributes name and age
		id, _ := record.Change.NewImage["Id"].Integer()
		month := record.Change.NewImage["Month"].String()
		interest, _ := record.Change.NewImage["Interest"].Integer()
		// updated_time := record.Change.NewImage["update_time"].String()

		endpoint := domain + "/" + index + "/" + "_doc" + "/" + strconv.Itoa(int(id))

		var schema = Schema{
			Id:       int(id),
			Month:    month,
			Interest: int(interest),
			// update_time: updated_time,
		}

		// struct converted into json object
		jsonObject, err := json.Marshal(schema)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				fmt.Println(aerr.Error())
			}
		}

		// JSON document to be included as the request body
// 		byte array to string conversion
		body := strings.NewReader(string(jsonObject))

		// An HTTP client for sending the request
		client := &http.Client{}

		// Form the HTTP requests

		req, err := http.NewRequest(http.MethodPut, endpoint, body)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				fmt.Println(aerr.Error())
			}
		}

		// You can probably infer Content-Type programmatically, but here, we just say that it's JSON
		req.Header.Add("Content-Type", "application/json")

		// signs the http request
		signer.Sign(req, body, service, region, time.Now())
		// fmt.Println("http request signed")

		resp, err := client.Do(req)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				fmt.Println(aerr.Error())
			}
		}

		// fmt.Println(resp.Header.Values)
		// fmt.Println(resp.Body)
		// fmt.Print(resp.Status + "\n")
	}
}

func main() {
	lambda.Start(handlerMethod)
}
