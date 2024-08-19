package tokenbucket

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"time"
)

const keyPrefix = "RATE_LIMIT"

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
		KeyPrefix:            keyPrefix,
		KeyCleanupInterval:   10 * time.Second,
		KeyCleanupBatchLimit: 10000,
	}
}

type Limiter struct {
	client  pgxUniversalClient
	Options Options
}

func NewLimiter(client pgxUniversalClient, options Options) *Limiter {
	ctx := context.Background()

	limiter := &Limiter{
		client:  client,
		Options: options,
	}

	if err := limiter.initializeSchema(ctx); err != nil {
		log.Fatalf("error initializing token bucket rate limiter scheme: %s", err)
	}

	return limiter
}

func (t *Limiter) ConsumeTokens(ctx context.Context, key string, limit Limit) (*Result, error) {
	row := t.client.QueryRow(ctx, consumeTokens, keyWithPrefix(t.Options.KeyPrefix, key), limit.RefillPerSec, limit.WindowSeconds)

	var tokens int
	err := row.Scan(&tokens)
	if err != nil {
		return nil, err
	}

	return &Result{
		Limit:  limit,
		Tokens: tokens,
	}, nil
}

func (t *Limiter) initializeSchema(ctx context.Context) error {
	schemaStatements := []string{
		createTokenBucketTable,
		createFunctionConsumeTokens,
	}

	tx, err := t.client.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err := tx.Rollback(ctx)
			if err != nil {
				return
			}
			return
		}
		err = tx.Commit(ctx)
	}()

	for _, stmt := range schemaStatements {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}
