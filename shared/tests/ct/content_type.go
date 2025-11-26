// Package ct provides HTTP Content-Type header constants.
package ct

// ContentType represents HTTP Content-Type header values.
type ContentType string

const (
	// JSON represents application/json content type.
	JSON ContentType = "application/json"

	// Text represents text/plain content type.
	Text ContentType = "text/plain"

	// DynamoDBJSON represents application/x-amz-json-1.0 content type
	// used by DynamoDB API.
	DynamoDBJSON ContentType = "application/x-amz-json-1.0"

	// HTML represents text/html content type.
	HTML ContentType = "text/html"

	// XML represents application/xml content type.
	XML ContentType = "application/xml"

	// FormURLEncoded represents application/x-www-form-urlencoded content type.
	FormURLEncoded ContentType = "application/x-www-form-urlencoded"

	// OctetStream represents application/octet-stream content type.
	OctetStream ContentType = "application/octet-stream"
)
