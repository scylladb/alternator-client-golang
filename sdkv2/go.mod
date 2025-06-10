module github.com/scylladb/alternator-client-golang/sdkv2

go 1.24.0

require (
	github.com/aws/aws-sdk-go-v2 v1.36.3
	github.com/aws/aws-sdk-go-v2/credentials v1.17.62
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.18.7
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.41.1
	github.com/aws/smithy-go v1.22.3
	github.com/scylladb/alternator-client-golang/shared v0.0.0-20250607211739-627953485187
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.25.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.15 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
