// Package apperr defines the application error kinds shared by all
// services and the mapping from those kinds to HTTP status codes.
package apperr

import "errors"

// Kind classifies an application error for the HTTP error mapper.
type Kind int

const (
	// KindValidation marks invalid input (HTTP 400).
	KindValidation Kind = iota
	// KindNotFound marks a missing resource (HTTP 404).
	KindNotFound
	// KindConflict marks a uniqueness conflict (HTTP 409).
	KindConflict
	// KindNotImplemented marks an operation that is not implemented yet
	// (HTTP 501).
	KindNotImplemented
)

// Error is an application error with a kind and a human-readable message.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Message }

// Validation returns a 400-mapped validation error.
func Validation(msg string) error { return &Error{Kind: KindValidation, Message: msg} }

// NotFound returns a 404-mapped not-found error.
func NotFound(msg string) error { return &Error{Kind: KindNotFound, Message: msg} }

// Conflict returns a 409-mapped conflict error.
func Conflict(msg string) error { return &Error{Kind: KindConflict, Message: msg} }

// NotImplemented returns a 501-mapped not-implemented error.
func NotImplemented(msg string) error { return &Error{Kind: KindNotImplemented, Message: msg} }

// HTTPStatus maps err to the HTTP status code used in the response body.
// Application errors map to their kind's status; anything else is a 500.
func HTTPStatus(err error) int {
	var appErr *Error
	if errors.As(err, &appErr) {
		switch appErr.Kind {
		case KindValidation:
			return 400
		case KindNotFound:
			return 404
		case KindConflict:
			return 409
		case KindNotImplemented:
			return 501
		}
	}
	return 500
}

// Message extracts a response-safe message from err.
func Message(err error) string {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Message
	}
	return "Internal server error"
}
