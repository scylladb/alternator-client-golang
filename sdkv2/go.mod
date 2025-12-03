module github.com/scylladb/alternator-client-golang/sdkv2

go 1.24.0

require (
	github.com/aws/aws-sdk-go-v2 v1.40.1
	github.com/aws/aws-sdk-go-v2/credentials v1.19.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.27
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.53.3
	github.com/aws/smithy-go v1.24.0
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.18.1
	github.com/scylladb/alternator-client-golang/shared v0.0.0-20250916125851-cc515eb951ad
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.15 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
