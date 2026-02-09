module github.com/scylladb/alternator-client-golang/sdkv2

go 1.24.0

require (
	github.com/aws/aws-sdk-go-v2 v1.41.1
	github.com/aws/aws-sdk-go-v2/credentials v1.19.7
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.31
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.54.0
	github.com/aws/smithy-go v1.24.0
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.18.4
	github.com/scylladb/alternator-client-golang/shared v1.0.5
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.17 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
