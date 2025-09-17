module github.com/scylladb/alternator-client-golang/sdkv1

go 1.24.0

require (
	github.com/aws/aws-sdk-go v1.55.8
	github.com/scylladb/alternator-client-golang/shared v0.0.0-20250916125851-cc515eb951ad
)

require (
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
