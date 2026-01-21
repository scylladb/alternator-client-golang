# GoLang Alternator client

## Glossary

- Alternator.
An DynamoDB API implemented on top of ScyllaDB backend.
Unlike AWS DynamoDB’s single endpoint, Alternator is distributed across multiple nodes.
Could be deployed anywhere: locally, on AWS, on any cloud provider.

- Client-side load balancing.
A method where the client selects which server (node) to send requests to,
rather than relying on a load balancing service.

- DynamoDB.
A managed NoSQL database service by AWS, typically accessed via a single regional endpoint.

- AWS Golang SDK.
The official AWS SDK for the Go programming language, used to interact with AWS services like DynamoDB.
Have two versions: [v1](https://github.com/aws/aws-sdk-go) and [v2](https://github.com/aws/aws-sdk-go-v2)

- DynamoDB/Alternator Endpoint.
The base URL a client connects to.
In AWS DynamoDB, this is typically something like http://dynamodb.us-east-1.amazonaws.com.
In DynamoDB it is any of Alternator nodes

- Datacenter (DC).
A physical or logical grouping of racks.
On Scylla Cloud in regular setup it represents cloud provider region where nodes are deployed.

- Rack.
A logical grouping akin to an availability zone within a datacenter.
On Scylla Cloud in regular setup it represents cloud provider availability zone where nodes are deployed.

## Introduction

This repo is a simple helper for AWS SDK, that allows seamlessly create a DynamoDB client that balance load across Alternator nodes.
There is a separate library every AWS SDK version:
- For [v1](https://github.com/aws/aws-sdk-go) - [sdkv1](sdkv1)
- For [v2](https://github.com/aws/aws-sdk-go-v2) - [sdkv2](sdkv2)

## Using the library

You create a regular `dynamodb.DynamoDB` client by one of the methods listed below and
the rest of the application can use this dynamodb client normally
this `db` object is thread-safe and can be used from multiple threads.

This client will send requests to an Alternator nodes, instead of AWS DynamoDB.

Every request performed on patched session will pick a different live
Alternator node to send it to.
Connections to every node will be kept alive even if no requests are being sent.

### Rack and Datacenter awareness

You can configure load balancer to target particular datacenter (region) or rack (availability zone) via `WithRack` and `WithDatacenter` options, like so:
```golang
    lb, err := alb.NewHelper([]string{"x.x.x.x"}, alb.WithRack("someRack"), alb.WithDatacenter("someDc1"))
```

Additionally, you can check if alternator cluster know targeted rack/datacenter:
```golang
	if err := lb.CheckIfRackAndDatacenterSetCorrectly(); err != nil {
		return fmt.Errorf("CheckIfRackAndDatacenterSetCorrectly() unexpectedly returned an error: %v", err)
	}
```

To check if cluster support datacenter/rack feature supported you can call `CheckIfRackDatacenterFeatureIsSupported`:
```golang
    supported, err := lb.CheckIfRackDatacenterFeatureIsSupported()
	if err != nil {
		return fmt.Errorf("failed to check if rack/dc feature is supported: %v", err)
	}
	if !supported {
        return fmt.Errorf("dc/rack feature is not supporte")
    }
```

### Create DynamoDB client

```golang
import (
	"fmt"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

    helper "github.com/scylladb/alternator-client-golang/sdkv2"
)

func main() {
    h, err := helper.NewHelper([]string{"x.x.x.x"}, helper.WithPort(9999), helper.WithCredentials("whatever", "secret"))
    if err != nil {
        panic(fmt.Sprintf("failed to create alternator helper: %v", err))
    }
    ddb, err := h.NewDynamoDB()
    if err != nil {
        panic(fmt.Sprintf("failed to create dynamodb client: %v", err))
    }
    _, _ = ddb.DeleteTable(...)
}
```

### Customizing AWS SDK config

Use `WithAWSConfigOptions` to tweak the generated `aws.Config` before building the DynamoDB client (e.g., adjust retryers or log mode). For AWS SDK v2:
```go
h, _ := helper.NewHelper(
    []string{"x.x.x.x"},
    helper.WithAWSConfigOptions(func(cfg *aws.Config) {
        cfg.RetryMaxAttempts = 5
    }),
)
```
For AWS SDK v1, call the same option but the callback receives `*aws.Config` from SDK v1.

### HTTP timeouts and retries

Use `WithHTTPClientTimeout` to set `http.Client.Timeout` for both Alternator data plane calls and the background live-nodes refreshes. The default mirrors Go’s `http.DefaultClient.Timeout` (zero, meaning no deadline). AWS SDK retries remain in effect, so each HTTP attempt can use the full timeout, and backoff occurs between attempts; total wall time can be up to `maxAttempts * timeout + sum_of_backoffs_between_attempts`. The timeout applies to each individual HTTP attempt, not to the entire sequence of retries. To further bound the end-to-end duration, you can also set a context deadline at the call site.

To bound a single DynamoDB query end-to-end, combine a finite HTTP timeout with a context deadline. For AWS SDK v2:
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

h, _ := helper.NewHelper(
    []string{"x.x.x.x"},
    helper.WithHTTPClientTimeout(2*time.Second),
)
ddb, _ := h.NewDynamoDB()
_, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("tbl"), Key: key})
```
SDK v1 users can apply the same pattern with the `*WithContext` methods (e.g., `GetItemWithContext`).

### HTTP connection pool settings

The library maintains a pool of idle HTTP connections to Alternator nodes for reuse, reducing latency and overhead. You can tune connection pooling behavior with the following options:

- **`WithMaxIdleHTTPConnections(value int)`**: Controls the maximum total number of idle connections across all hosts. Default is `100`. Set to `0` to disable connection reuse entirely.

- **`WithMaxIdleHTTPConnectionsPerHost(value int)`**: Controls the maximum number of idle connections per host. Default is `http.DefaultMaxIdleConnsPerHost` which is `2`. Increase this value when making many concurrent requests to the same node.

- **`WithIdleHTTPConnectionTimeout(value time.Duration)`**: Controls how long idle connections remain in the pool before being closed. Default is `6 hours`. Shorter timeouts free resources faster but may increase connection setup overhead.

Example:
```go
h, _ := helper.NewHelper(
    []string{"x.x.x.x"},
    helper.WithMaxIdleHTTPConnections(200),
    helper.WithMaxIdleHTTPConnectionsPerHost(10),
    helper.WithIdleHTTPConnectionTimeout(30*time.Minute),
)
```

## Distinctive features

### Headers optimization

Alternator does not use all the headers that are normally used by DynamoDB.
So, it is possible to instruct client to delete unused http headers from the request to reduce network footprint.
Artificial testing showed that this technic can reduce outgoing traffic up to 56%, depending on workload and encryption.

It is supported only for AWS SDKv2, example how to enable it:
```go
    h, err := helper.NewHelper(
		[]string{"x.x.x.x"},
	    helper.WithPort(9999),
		helper.WithCredentials("whatever", "secret"),
		helper.WithOptimizeHeaders(true),
	)
    if err != nil {
        panic(fmt.Sprintf("failed to create alternator helper: %v", err))
    }
```

### Request compression

It is possible to enable request compression with:
```go
    h, err := helper.NewHelper(
		[]string{"x.x.x.x"},
	    helper.WithPort(9999),
		helper.WithCredentials("whatever", "secret"),
		helper.WithRequestCompression(NewGzipConfig().GzipRequestCompressor()),
	)
    if err != nil {
        panic(fmt.Sprintf("failed to create alternator helper: %v", err))
    }
```
For now only Gzip compression is supported in the future there is a possiblity to add more.

#### GZIP compression

To create a new Gzip configuration, use `NewGzipConfig()`. You can also set compression level via `WithLevel()` option to control the trade-off between compression speed and compression ratio.

### Disabling Node Health Tracking

By default, the library tracks node health and temporarily quarantines nodes that experience connection errors. This helps route traffic away from unhealthy nodes. However, in some scenarios you may want to disable this behavior:

- When using an external load balancer that already handles node health
- In testing environments where you want predictable round-robin behavior
- When you prefer to let AWS SDK retries handle transient failures

To disable node health tracking:
```go
h, err := helper.NewHelper(
    []string{"x.x.x.x"},
    helper.WithNodeHealthStoreConfig(nodeshealth.NodeHealthStoreConfig{
        Disabled: true,
    }),
)
```

When disabled:
- All discovered nodes remain active regardless of errors
- No nodes are ever quarantined
- Node discovery (add/remove) continues to work normally
- AWS SDK retry mechanisms still handle transient failures

### KeyRouteAffinity

When using Lightweight Transactions (LWT) in ScyllaDB/Alternator, routing requests for the same partition key to the same coordinator node can significantly improve performance.
This is because LWT operations require consensus among replicas, and using the same coordinator reduces coordination overhead.
KeyRouteAffnity is a way to reduce this overhead by ensuring that two queries targeting same partition key will be scheduled to the same coordinator.
Instead of using random selection of nodes in a round-robin fashion it provides a way to have a deterministic, idempotent selection of nodes basing on PK.

#### Alternator Write Isolation Modes

ScyllaDB's Alternator supports different write isolation modes configured via `alternator_write_isolation`:

- **`always`**: All write operations use LWT (Paxos consensus). Maximum consistency but higher latency.
- **`only_rmw_uses_lwt`**: Only Read-Modify-Write operations (UpdateItem with conditions, DeleteItem with conditions) use LWT. This is the **recommended setting** for most use cases.
- **`forbid_rmw`**: LWTs are completely disabled. Conditional operations will fail.
- **`unsafe_rmw`**: Unsafe - does not use LWT for RMW operations.

#### When to Use KeyRouteAffinity

Enable KeyRouteAffinity when:
- Your Alternator cluster is configured with `alternator_write_isolation: only_rmw_uses_lwt` (use `KeyRouteAffinityRMW`) or `always` (use `KeyRouteAffinityAnyWrite`)
- You perform conditional updates/deletes on the same items repeatedly
- You want to optimize LWT performance by ensuring the same coordinator handles requests for the same partition key

#### Configuration Options

There are three KeyRouteAffinity modes:

1. **`KeyRouteAffinityNone`** (default): Disabled. Requests are distributed randomly across nodes.
2. **`KeyRouteAffinityRMW`**: Enables route affinity for conditional write operations, operations that needs read before write.
3. **`KeyRouteAffinityAnyWrite`**: Enables routing optimization for all write operations.

#### Automatic Partition Key Discovery

The driver automatically discovers the partition key.
It periodically runs DescribeTable in the background to retrieve the partition key name.
Until the partition key is discovered, operations run without partition-key optimizations.

#### Pre-Configuring Partition Keys with WithPkInfo

If you don't want to wait till driver automatically discovers partition key you can use `WithPkInfo` to pre-configure the 
partition key column name for tables you are working with:
```go
h, err := helper.NewHelper(
    []string{"x.x.x.x"},
    helper.WithPort(9999),
    helper.WithCredentials("whatever", "secret"),
    helper.WithKeyRouteAffinity(
        helper.NewKeyRouteAffinityConfig(helper.KeyRouteAffinityWrite).
            WithPkInfo(map[string]string{
                "users":  "userId",
            }),
    ),
)
```

### Decrypting TLS

Read wireshark wiki regarding decrypting TLS traffic: https://wiki.wireshark.org/TLS#using-the-pre-master-secret
In order to obtain pre master key secrets, you need to provide a file writer into `alb.WithKeyLogWriter`, example:

```go
	keyWriter, err := os.OpenFile("/tmp/pre-master-key.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
    if err != nil {
        panic("Error opening key writer: " + err.Error())
	}
	defer keyWriter.Close()
	lb, err := alb.NewHelper(knownNodes, alb.WithScheme("https"), alb.WithPort(httpsPort), alb.WithIgnoreServerCertificateError(true), alb.WithKeyLogWriter(keyWriter))
```

Then you need to configure your traffic analyzer to read pre master key secrets from this file.

## Examples

You can find examples in [sdkv1/helper_test.go](sdkv1/helper_test.go) and [sdkv2/helper_test.go](sdkv2/helper_test.go)
