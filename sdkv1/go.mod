module github.com/scylladb/alternator-client-golang/sdkv1

go 1.24.0

require (
	github.com/aws/aws-sdk-go v1.55.8
	github.com/klauspost/compress v1.18.4
	github.com/scylladb/alternator-client-golang/shared v1.0.5
)

require (
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
)

replace github.com/scylladb/alternator-client-golang/shared => ../shared
