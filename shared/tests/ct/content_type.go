// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
