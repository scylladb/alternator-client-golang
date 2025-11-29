// Package errs contains all the errors shared between two clients
package errs

import "errors"

var (
	// ErrCtxHasNoQueryPlan is an error signaling that request context does not have query plan assign to it
	// which is a bug and needs to be reported
	ErrCtxHasNoQueryPlan = errors.New("no query plan found on the context, it is a bug, please report")
	// ErrCtxHasNoNode is a similar error regarding a target node
	ErrCtxHasNoNode = errors.New("no node found on the context, it is a bug, please report")
	// ErrQueryPlanExhausted signals that query plan come to an end and have no more nodes in it
	ErrQueryPlanExhausted = errors.New("query plan has been exhausted")
)
