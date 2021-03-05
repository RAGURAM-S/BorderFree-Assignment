package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

type ResponseStruct struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index  string  `json:"_index"`
			Type   string  `json:"_type"`
			ID     string  `json:"_id"`
			Score  float64 `json:"_score"`
			Source struct {
				ID       int    `json:"Id"`
				Month    string `json:"Month"`
				Interest int    `json:"Interest"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type Schema struct {
	Id       int    `json: "Id"`
	Month    string `json: "Month"`
	Interest int    `json: "Interest"`
}

const (
	domain  = ""
	index   = "updates-cupcakes"
	region  = "us-east-2"
	service = "es"
)

func apiHandler(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	urlString := domain + "/" + index + "/_doc" + "/_search?size=1000"
	// response, err := http.Get(urlString)

	// if err != nil {
	// 	log.Fatal(err)
	// }

	client := &http.Client{Timeout: 10 * time.Second}

	credentials := credentials.NewStaticCredentials(os.Getenv("ACCESS_KEY_"), os.Getenv("SECRET_KEY_"), "")
	signer := v4.NewSigner(credentials)

	request, err := http.NewRequest(http.MethodGet, urlString, nil)

	if err != nil {
		log.Fatal(err)
	}

	signer.Sign(request, nil, service, region, time.Now())

	response, err := client.Do(request)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println(aerr.Error())
		}
	}

	// data, _ := ioutil.ReadAll(response.Body)
	// fmt.Println(string(data))

	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)

	var responseStruct ResponseStruct

	err = decoder.Decode(&responseStruct)
	if err != nil {
		log.Fatal(err)
	}

	array := make([]Schema, 0)

	// iterates through the Hits[]
	for _, hits := range responseStruct.Hits.Hits {
		// fmt.Printf("%d: %d %s %d\n", i, hits.Source.ID, hits.Source.Month, hits.Source.Interest)
		// fmt.Println(i)

		var schema Schema = Schema{
			Id:       hits.Source.ID,
			Month:    hits.Source.Month,
			Interest: hits.Source.Interest,
		}
// 		appends the struct to an array
		array = append(array, schema)
	}

	for i := 0; i < len(array); i++ {
		fmt.Println(array[i])
	}

	// converts the array to bytes
	responseBodyBytes := new(bytes.Buffer)
	json.NewEncoder(responseBodyBytes).Encode(array)

	// fmt.Println(string(responseBodyBytes.Bytes()))

	apiResponse := events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
// 			cors
			"Access-Control-Allow-Origin": "*",
			"Content-Type": "application/json",
		},
		Body: string(responseBodyBytes.Bytes()),
	}

	return apiResponse, nil

}

func main() {
	lambda.Start(apiHandler)
}
