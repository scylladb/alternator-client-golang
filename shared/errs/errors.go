// Package errs contains all the errors shared between two clients
package errs

import "errors"

var (
	// ErrCtxHasNoQueryPlan is an error signaling that request context does not have query plan assigned to it
	// which is a bug and needs to be reported
	ErrCtxHasNoQueryPlan = errors.New("no query plan found on the context, it is a bug, please report")
	// ErrCtxHasNoNode is a similar error regarding a target node
	ErrCtxHasNoNode = errors.New("no node found on the context, it is a bug, please report")
	// ErrQueryPlanExhausted signals that query plan has come to an end and has no more nodes in it
	ErrQueryPlanExhausted = errors.New("query plan has been exhausted")
)
