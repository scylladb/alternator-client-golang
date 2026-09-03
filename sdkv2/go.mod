module github.com/scylladb/alternator-client-golang/sdkv2

go 1.25

require (
	github.com/aws/aws-sdk-go-v2 v1.45.1
	github.com/aws/aws-sdk-go-v2/credentials v1.20.2
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.21.2
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.66.0
	github.com/aws/smithy-go v1.28.1
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.20.0
	github.com/scylladb/alternator-client-golang/shared v1.0.6
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.39.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
