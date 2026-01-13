// Package logx provides a lightweight, implementation-agnostic logging API.
//
// It defines a universal Logger interface with support for structured logging,
// log levels (debug, info, warn, error), and contextual attributes.
// Implementations can wrap any underlying logging library while exposing
// a consistent API to application code.
//
// The package provides:
//   - Level: predefined logging severity levels.
//   - Attr: a key/value pair for structured log fields.
//   - A: helper function to quickly create an Attr.
//   - Noop: a Logger implementation that discards all log messages.
package logx

// Level represents the severity of a log message.
type Level int

const (
	// DebugLevel is a level for messages for verbose output useful in development.
	DebugLevel Level = iota
	// InfoLevel is a level for informational messages for normal application operation.
	InfoLevel
	// WarnLevel is a level for unexpected situations that are recoverable.
	WarnLevel
	// ErrorLevel is a level for problems that require attention.
	ErrorLevel
)

// Attr represents a key/value pair used for structured logging.
type Attr struct {
	Key   string // The attribute name.
	Value any    // The attribute value.
}

// A creates an Attr from a key and value.
func A(k string, v any) Attr { return Attr{Key: k, Value: v} }

// Logger describes a universal, implementation-agnostic logging API.
type Logger interface {
	// Log logs a message at the given severity level with optional structured attributes.
	Log(lvl Level, msg string, attrs ...Attr)

	// Debug logs a debug-level message with optional attributes.
	Debug(msg string, attrs ...Attr)
	// Info logs an informational message with optional attributes.
	Info(msg string, attrs ...Attr)
	// Warn logs a warning message with optional attributes.
	Warn(msg string, attrs ...Attr)
	// Error logs an error message with optional attributes.
	Error(msg string, attrs ...Attr)

	// With returns a Logger that includes the given attributes with all future log messages.
	With(attrs ...Attr) Logger
	// Named returns a Logger with an additional name identifier for scoping logs.
	Named(name string) Logger

	// Enabled reports whether logging is enabled for the given level.
	Enabled(lvl Level) bool
}

// Noop is a Logger implementation that discards all log messages.
type Noop struct{}

// Log discards the log message and attributes.
func (Noop) Log(Level, string, ...Attr) {}

// Debug discards the debug message and attributes.
func (Noop) Debug(string, ...Attr) {}

// Info discards the informational message and attributes.
func (Noop) Info(string, ...Attr) {}

// Warn discards the warning message and attributes.
func (Noop) Warn(string, ...Attr) {}

// Error discards the error message and attributes.
func (Noop) Error(string, ...Attr) {}

// With returns the same Noop logger, ignoring the provided attributes.
func (n Noop) With(...Attr) Logger { return n }

// Named returns the same Noop logger, ignoring the provided name.
func (n Noop) Named(string) Logger { return n }

// Enabled always returns false, indicating logging is disabled.
func (Noop) Enabled(Level) bool { return false }

// Error wraps an error with Attr, so that it could be attached to a log message
func Error(err error) Attr {
	return NamedError("error", err)
}

// NamedError constructs an attr that lazily stores err.Error() under the
// provided key. Errors which also implement fmt.Formatter (like those produced
// by github.com/pkg/errors) will also have their verbose representation stored
// under key+"Verbose". If passed a nil error, the field is a no-op.
//
// For the common case in which the key is simply "error", the Error function
// is shorter and less repetitive.
func NamedError(key string, err error) Attr {
	return Attr{Key: key, Value: err}
}
