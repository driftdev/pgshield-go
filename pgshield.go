package pgshield

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
)

const defaultKeyPrefix = "RATE_LIMIT"

type pgxUniversalClient interface {
	Exec(ctx context.Context, stmt string, args ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, stmt string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, stmt string, args ...interface{}) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Options holds the configuration settings for the LeakyBucketLimiter.
// It includes parameters for key prefix, cleanup intervals, and batch limits.
type Options struct {
	// KeyPrefix is the prefix for keys managed by the limiter.
	// By default, it is set to "RATE_LIMIT", but it can be customized.
	KeyPrefix string
	// KeyCleanupInterval is the interval at which expired keys are cleaned up.
	// Default is 10 seconds.
	KeyCleanupInterval time.Duration
	// KeyCleanupBatchLimit is the maximum number of keys to delete per cleanup run.
	// Default is 10000.
	KeyCleanupBatchLimit int
}

// DefaultOptions returns an Options struct initialized with default values for limiter configuration settings.
// The default values are designed to provide a basic configuration suitable for many scenarios.
// Each field in the Options struct is set to a default value, as documented below:
//
// Defaults:
//
//   - KeyPrefix: The prefix used for keys managed by the limiter. Default is set to "RATE_LIMIT".
//   - KeyCleanupInterval: The interval between cleanup operations for expired key cleanup. Default is 10 seconds.
//   - KeyCleanupBatchLimit: The maximum number of keys to delete during each cleanup run. Default is 10000.
func DefaultOptions() Options {
	return Options{
		KeyPrefix:            defaultKeyPrefix,
		KeyCleanupInterval:   10 * time.Second,
		KeyCleanupBatchLimit: 10000,
	}
}

// dur converts a floating-point number representing seconds into a time.Duration.
// If the input is -1, the function returns -1, indicating an indefinite or unbounded duration.
func dur(f float64) time.Duration {
	if f == -1 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}

// keyWithPrefix creates a key by appending a specified key to a given prefix.
// This helps to ensure that the keys used for rate limiting are namespaced properly,
func keyWithPrefix(keyPrefix string, key string) string {
	return keyPrefix + ":" + key
}
