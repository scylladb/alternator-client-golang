// Package resp provides API to create http responses to test Alternator client library.
package resp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/scylladb/alternator-client-golang/shared/tests/ct"
)

// Builder provides a fluent API for building HTTP responses for testing.
// It supports chaining methods to construct responses with various status codes,
// headers, and body content.
//
// Example usage:
//
//	resp := New().
//	    Status(200).
//	    ContentType(ct.JSON).
//	    Body(`{"key": "value"}`).
//	    Build()
type Builder struct {
	statusCode int
	headers    http.Header
	body       []byte
	request    *http.Request
	err        error
}

// New creates a new Builder with default values.
// Default status code is 200 (OK).
func New() *Builder {
	return &Builder{
		statusCode: http.StatusOK,
		headers:    make(http.Header),
	}
}

// Status sets the HTTP status code for the response.
func (rb *Builder) Status(code int) *Builder {
	rb.statusCode = code
	return rb
}

// Header sets a single header value. If the header already exists, it will be replaced.
func (rb *Builder) Header(key, value string) *Builder {
	rb.headers.Set(key, value)
	return rb
}

// Headers sets multiple header values at once. Existing headers are preserved.
func (rb *Builder) Headers(headers http.Header) *Builder {
	for key, values := range headers {
		for _, value := range values {
			rb.headers.Set(key, value)
		}
	}
	return rb
}

// Body sets the response body content.
func (rb *Builder) Body(body string) *Builder {
	rb.body = []byte(body)
	return rb
}

// JSONBody serialize object to json and sets the response body.
func (rb *Builder) JSONBody(body interface{}) *Builder {
	js, err := json.Marshal(body)
	if err != nil {
		panic("cannot marshal JSON body: " + err.Error())
	}
	rb.body = js
	return rb
}

// Request associates the original HTTP request with this response.
func (rb *Builder) Request(req *http.Request) *Builder {
	rb.request = req
	return rb
}

// Error sets an error to be returned along with a nil response.
// When this is set, Build() will return (nil, error).
func (rb *Builder) Error(err error) *Builder {
	rb.err = err
	return rb
}

// ContentType sets the Content-Type header using predefined content type constants.
func (rb *Builder) ContentType(contentType ct.ContentType) *Builder {
	return rb.Header("Content-Type", string(contentType))
}

// OK is a convenience method to set status to 200.
func (rb *Builder) OK() *Builder {
	return rb.Status(http.StatusOK)
}

// NotFound is a convenience method to set status to 404.
func (rb *Builder) NotFound() *Builder {
	return rb.Status(http.StatusNotFound)
}

// InternalServerError is a convenience method to set status to 500.
func (rb *Builder) InternalServerError() *Builder {
	return rb.Status(http.StatusInternalServerError)
}

// BadRequest is a convenience method to set status to 400.
func (rb *Builder) BadRequest() *Builder {
	return rb.Status(http.StatusBadRequest)
}

// Unauthorized is a convenience method to set status to 401.
func (rb *Builder) Unauthorized() *Builder {
	return rb.Status(http.StatusUnauthorized)
}

// ServiceUnavailable is a convenience method to set status to 503.
func (rb *Builder) ServiceUnavailable() *Builder {
	return rb.Status(http.StatusServiceUnavailable)
}

// Build constructs and returns the http.Response and any error.
// If an error was set via Error(), returns (nil, error).
// Otherwise, returns a fully constructed http.Response.
func (rb *Builder) Build() (*http.Response, error) {
	if rb.err != nil {
		return nil, rb.err
	}

	return &http.Response{
		StatusCode: rb.statusCode,
		Header:     rb.headers,
		Body:       io.NopCloser(bytes.NewReader(rb.body)),
		Request:    rb.request,
	}, nil
}

// Common response patterns for convenience

// AlternatorNodesResponse creates a response for /localnodes endpoint
// with the given list of node hostnames.
func AlternatorNodesResponse(nodes []string, req *http.Request) (*http.Response, error) {
	return New().
		OK().
		ContentType(ct.JSON).
		JSONBody(nodes).
		Request(req).
		Build()
}

// HealthCheckResponse creates a successful health check response.
func HealthCheckResponse(req *http.Request) (*http.Response, error) {
	return New().
		OK().
		ContentType(ct.Text).
		Body("OK").
		Request(req).
		Build()
}

// DynamoDBListTablesResponse creates a mock DynamoDB ListTables response
// with the given table names.
func DynamoDBListTablesResponse(tableNames []string, req *http.Request) (*http.Response, error) {
	body := `{"TableNames":["` + strings.Join(tableNames, `","`) + `"]}`
	return New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(body).
		Request(req).
		Build()
}

// DynamoDBPutItemResponse creates a mock DynamoDB PutItem response
func DynamoDBPutItemResponse(req *http.Request) (*http.Response, error) {
	body := `{}`
	return New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(body).
		Request(req).
		Build()
}

// DynamoDBGetItemResponse creates a mock DynamoDB GetItem response
func DynamoDBGetItemResponse(item interface{}, req *http.Request) (*http.Response, error) {
	body := `{"Item":{}}`
	if item != nil {
		itemJSON, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		body = `{"Item":` + string(itemJSON) + `}`
	}
	return New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(body).
		Request(req).
		Build()
}

// DynamoDBUpdateItemResponse creates a mock DynamoDB UpdateItem response
func DynamoDBUpdateItemResponse(req *http.Request) (*http.Response, error) {
	body := `{}`
	return New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(body).
		Request(req).
		Build()
}

// DynamoDBDeleteItemResponse creates a mock DynamoDB DeleteItem response
func DynamoDBDeleteItemResponse(req *http.Request) (*http.Response, error) {
	body := `{}`
	return New().
		OK().
		ContentType(ct.DynamoDBJSON).
		Body(body).
		Request(req).
		Build()
}
