module github.com/scylladb/alternator-client-golang/sdkv2

go 1.24.0

require (
	github.com/aws/aws-sdk-go-v2 v1.42.0
	github.com/aws/aws-sdk-go-v2/credentials v1.19.24
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.48
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.59.0
	github.com/aws/smithy-go v1.27.2
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.18.6
	github.com/scylladb/alternator-client-golang/shared v1.0.6
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.34.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.6 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
