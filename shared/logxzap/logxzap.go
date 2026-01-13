// Package logxzap provides an implementation of the logx.Logger interface
// backed by the Uber Zap logging library.
//
// It allows applications to use the generic logx logging API while leveraging
// Zap's high-performance structured logging capabilities. This helps maintain
// a consistent logging abstraction across different logger backends.
//
// The package includes:
//   - Logger: an adapter that wraps zap.Logger and implements logx.Logger.
//   - New: creates a Logger from an existing zap.Logger.
//   - DefaultLogger: returns a preconfigured console logger at Debug level.
//
// This package is intended for use where you want to:
//   - Keep application logging code decoupled from the underlying logging library.
//   - Maintain structured, leveled logging with minimal overhead.
//   - Reuse the same logging interface (logx.Logger) across different backends.
//
// Example:
//
//	import (
//	    "github.com/scylladb/alternator-client-golang/shared/logx"
//	    "github.com/scylladb/alternator-client-golang/shared/logxzap"
//	)
//
//	func main() {
//	    logger := logxzap.DefaultLogger()
//	    logger.Info("Application started", logx.A("version", "1.0.0"))
//	}
package logxzap

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/alternator-client-golang/shared/logx"
)

// Logger is an adapter that implements the logx.Logger interface
// using an underlying zap.Logger instance.
type Logger struct {
	z *zap.Logger
}

// New creates a new Logger that wraps the given zap.Logger.
func New(z *zap.Logger) *Logger { return &Logger{z: z} }

// Log writes a log entry at the specified level with the given message
// and optional structured attributes. If the level is disabled, the log
// is skipped without formatting cost.
func (l *Logger) Log(lvl logx.Level, msg string, attrs ...logx.Attr) {
	if ce := l.z.Check(toZapLevel(lvl), msg); ce != nil {
		ce.Write(toZapFields(attrs)...)
	}
}

// Debug logs a message at the Debug level with optional structured attributes.
func (l *Logger) Debug(msg string, attrs ...logx.Attr) {
	l.Log(logx.DebugLevel, msg, attrs...)
}

// Info logs a message at the Info level with optional structured attributes.
func (l *Logger) Info(msg string, attrs ...logx.Attr) {
	l.Log(logx.InfoLevel, msg, attrs...)
}

// Warn logs a message at the Warn level with optional structured attributes.
func (l *Logger) Warn(msg string, attrs ...logx.Attr) {
	l.Log(logx.WarnLevel, msg, attrs...)
}

// Error logs a message at the Error level with optional structured attributes.
func (l *Logger) Error(msg string, attrs ...logx.Attr) {
	l.Log(logx.ErrorLevel, msg, attrs...)
}

// With returns a new Logger instance that includes the given attributes
// in all subsequent log entries.
func (l *Logger) With(attrs ...logx.Attr) logx.Logger {
	return &Logger{z: l.z.With(toZapFields(attrs)...)}
}

// Named returns a new Logger instance with an additional name scope.
// The name appears in the log output and helps identify the log source.
func (l *Logger) Named(name string) logx.Logger {
	return &Logger{z: l.z.Named(name)}
}

// Enabled reports whether the specified log level is enabled.
func (l *Logger) Enabled(lvl logx.Level) bool {
	return l.z.Core().Enabled(toZapLevel(lvl))
}

// toZapLevel converts a logx.Level to the corresponding zapcore.Level.
func toZapLevel(l logx.Level) zapcore.Level {
	switch l {
	case logx.DebugLevel:
		return zapcore.DebugLevel
	case logx.InfoLevel:
		return zapcore.InfoLevel
	case logx.WarnLevel:
		return zapcore.WarnLevel
	default:
		return zapcore.ErrorLevel
	}
}

// toZapFields converts logx.Attr values to zap.Field values.
func toZapFields(attrs []logx.Attr) []zap.Field {
	if len(attrs) == 0 {
		return nil
	}
	fs := make([]zap.Field, 0, len(attrs))
	for _, a := range attrs {
		fs = append(fs, zap.Any(a.Key, a.Value))
	}
	return fs
}

// DefaultLogger creates a new Logger that writes to standard output
// using zap's console encoder with ISO8601 timestamps and caller information.
// Logging is enabled at the Debug level and above.
func DefaultLogger() logx.Logger {
	cfg := zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	consoleEncoder := zapcore.NewConsoleEncoder(cfg)
	core := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel)

	return New(zap.New(core, zap.AddCaller()))
}
