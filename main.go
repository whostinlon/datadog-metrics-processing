package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"time"

	datadog "github.com/DataDog/datadog-api-client-go/api/v1/datadog"
)

type datadogResponse struct {
	Series []struct {
		Pointlist [][]*int `json:"pointlist,omitempty"` // PointList is a slice containing [timestamp, value] slices
		TagSet    []string `json:"tag_set,omitempty"`   // TagSet is a slice containing offer data [offer_id, product_type]
	} `json:"series,omitempty"`
}

const (
	tagSetOfferID = iota
	tagSetProductType
)

/* Query Datadog for failed and successful purchase data */
func queryAllMetrics(ctx *context.Context, numberOfDaysBack int, apiClient *datadog.APIClient, category string) (datadog.MetricsQueryResponse, *http.Response, error) {
	if category == "success" || category == "failure" {
		response, r, err := apiClient.MetricsApi.QueryMetrics(*ctx, time.Now().AddDate(0, 0, -numberOfDaysBack).Unix(), time.Now().Unix(),
			`sum:prometheus.payflow_subscription_`+category+`_total{env:prd} by {offer_id, product_type}.as_count()`)
		if err != nil {
			return datadog.MetricsQueryResponse{}, r, fmt.Errorf("failed to get data from payflowV2: %v: %w", err, r)
		}
		return response, r, err
	}
	return datadog.MetricsQueryResponse{}, nil, fmt.Errorf("invalid category")
}

/* Function to fetch all offers Data (to be presented in the Search Offers Tab) */
func fetchAllData(numberOfDaysBack int, offerIDs ...string) (map[string]map[string]interface{}, error) {
	if os.Getenv("DD_API_KEY") == "" || os.Getenv("DD_APP_KEY") == "" { // Datadog API Client requirement
		return nil, fmt.Errorf("failed to fetch environment variables")
	}

	ctx := datadog.NewDefaultContext(context.Background())
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)

	successResp, responsePointer, err := queryAllMetrics(&ctx, numberOfDaysBack, apiClient, "success")
	if err != nil {
		return nil, fmt.Errorf("failed to get success purchase data: %v: %w", responsePointer, err)
	}

	failureResp, responsePointer, err := queryAllMetrics(&ctx, numberOfDaysBack, apiClient, "failure")
	if err != nil {
		return nil, fmt.Errorf("failed to get failed purchase data: %v: %w", responsePointer, err)
	}

	successResponseContent, err := json.MarshalIndent(successResp, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal success response: %w", err)
	}

	failureResponseContent, err := json.MarshalIndent(failureResp, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal failure response: %w", err)
	}

	var successData datadogResponse
	var failureData datadogResponse

	err = json.Unmarshal(successResponseContent, &successData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal success response: %w", err)
	}

	err = json.Unmarshal(failureResponseContent, &failureData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal failure response: %w", err)
	}

	var payflowConversion float64       // Payflow Conversion is the conversion rate of successful purchases to total purchases as reported from Payflow
	re, err := regexp.Compile(`.+_.+:`) // Regex to match the offer_id and product_type prefixes in TagSet

	if err != nil {
		return nil, fmt.Errorf("failed to compile regex: %w", err)
	}

	result := make(map[string]map[string]interface{})

	for _, series := range successData.Series {
		/* Check if series.TagSet matches offerIDs */
		if len(offerIDs) != 0 {
			var success int
			var failure int
			offerID := re.ReplaceAllString(series.TagSet[tagSetOfferID], "")
			productType := re.ReplaceAllString(series.TagSet[tagSetProductType], "")

			for _, pointlist := range series.Pointlist {
				if pointlist[1] != nil {
					success += *pointlist[1]
				}
			}

			for _, fs := range failureData.Series { // Find correspoding series in failureData
				if fs.TagSet[tagSetOfferID] == series.TagSet[tagSetOfferID] {
					for _, pointlist := range fs.Pointlist {
						if pointlist[1] != nil {
							failure += *pointlist[1]
						}
					}
				}
			}

			if success+failure == 0 {
				payflowConversion = 0 // Division by zero and NaN reset to 0
			} else {
				payflowConversion = float64(success) / float64(success+failure) * 100
			}

			result[offerID] = map[string]interface{}{
				"success":           success,
				"failure":           failure,
				"sum":               success + failure,
				"payflowConversion": payflowConversion,
				"productType":       productType,
			}
		}

		return result, nil
	}
	return nil, err
}

func main() {
	result, error := fetchAllData(7)
	if error != nil {
		fmt.Println(error)
	} else {
		fmt.Println(result)
	}
}
