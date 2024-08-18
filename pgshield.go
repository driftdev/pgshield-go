package pgshield

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"time"
)

const defaultKeyPrefix = "RATE_LIMIT"

type pgxUniversalClient interface {
	Exec(ctx context.Context, stmt string, args ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, stmt string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, stmt string, args ...interface{}) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Options holds the configuration settings for the Limiter.
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

// Limiter is a struct that represents a rate limiter utilizing a pgx client.
// It encapsulates the pgxUniversalClient for database operations and configuration Options for the limiter.
type Limiter struct {
	client  pgxUniversalClient
	Options Options
}

// NewLimiter creates a new Limiter instance with the provided pgx client and options.
// It initializes the Limiter struct with the given client and options, ensuring that the key cleanup interval
// and batch limit are greater than 0. If either of these values is invalid, the function logs a fatal error
// and advises to use DefaultOptions() for proper configuration.
//
// Parameters:
//   - client: A pgxUniversalClient that implements the pgx.Conn or pgxpool.Conn interface for database operations.
//   - options: An Options struct that holds various limiter configuration settings.
//     If KeyCleanupInterval or KeyCleanupBatchLimit is set to 0 or less, the function will log a fatal error
//     and terminate, advising to use DefaultOptions() to get default values for these fields.
//
// Returns:
// - *Limiter: A pointer to the newly created Limiter instance.
//
// Usage:
// To create a new limiter instance, first configure the Options using either DefaultOptions() or by specifying custom values.
// Then, pass the pgx or pgxpool client and the options to NewLimiter:
//
// Example:
//
//	opts := DefaultOptions()
//	pgxClient := pgxpool.New(...)
//	limiter := NewLimiter(pgxClient, opts)
//
// If KeyCleanupInterval or KeyCleanupBatchLimit is not properly set (i.e., is 0 or less), the function will terminate
// with a fatal error, ensuring that these essential configuration values are provided.
func NewLimiter(client pgxUniversalClient, options Options) *Limiter {
	ctx := context.Background()

	if options.KeyCleanupInterval <= 0 || options.KeyCleanupBatchLimit <= 0 {
		log.Fatalf("pgshield options error: KeyCleanupInterval and KeyCleanupBatchLimit must be greater than 0. You should call DefaultOptions() to get default options")
	}

	limiter := &Limiter{
		client:  client,
		Options: options,
	}

	if err := limiter.initializeSchema(ctx); err != nil {
		log.Fatalf("error initializing pgshield schema: %v", err)
	}
	log.Println("pgshield schema and functions initialized successfully")

	go limiter.startCleanup(ctx, options.KeyCleanupInterval)

	return limiter
}

// Allow attempts to allow a single request for a given key under a rate-limiting
// scheme defined by the `pgshield.Limit` struct. This is a convenience method that
// calls `AllowN` with `n` set to 1, meaning it checks if just one request can
// be made at the current time.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the entity being rate-limited (e.g., user ID, IP address).
//   - limit: A `pgshield.Limit` struct defining the rate (requests per period) and burst (maximum burst size).
//     This can be created using helper functions such as `pgshield.PerSecond`, `pgshield.PerMinute`, or `pgshield.PerHour`.
//
// Returns:
//   - *Result: A struct containing information about the rate limiting state, such as the number
//     of requests allowed, remaining, and times for retry and reset.
//   - error: If an error occurs while executing the underlying `AllowN` method or parsing the result, it is returned.
//
// Example:
//
//	// Create a rate limit of 5 requests per second
//	limit := pgshield.PerSecond(5)
//
//	result, err := Allow(ctx, "user_1234", limit)
//	if err != nil {
//	    log.Fatalf("Failed to check rate limit: %v", err)
//	}
//
//	if result.Allowed > 0 {
//	    log.Println("Request allowed.")
//	} else {
//	    log.Printf("Rate limit exceeded. Retry after %v, reset after %v.", result.RetryAfter, result.ResetAfter)
//	}
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (*Result, error) {
	return l.AllowN(ctx, key, limit, 1)
}

// AllowN attempts to allow `n` requests for a given key under a rate-limiting
// scheme defined by the `Limit` struct. This method uses a Lua script to
// calculate whether the requests can be allowed based on the current rate limit state.
//
// The `pgshield.Limit` struct can be easily created using helper functions like `pgshield.PerSecond`,
// `pgshield.PerMinute`, and `pgshield.PerHour`, which define the rate limit in terms of requests
// per second, minute, or hour respectively. Each helper function sets the `Burst`
// to be equal to the `Rate`, meaning that the system can handle a burst of up to
// `Rate` requests in one period.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the entity being rate-limited (e.g., user ID, IP address).
//   - limit: A `pgshield.Limit` struct defining the rate (requests per period) and burst (maximum burst size).
//   - n: The number of requests to attempt to allow.
//
// Returns:
//   - *Result: A struct containing information about the rate limiting state, such as the number
//     of requests allowed, remaining, and times for retry and reset.
//   - error: If an error occurs while executing the Lua script or parsing the result, it is returned.
//
// Example:
//
//	// Create a rate limit of 5 requests per second
//	limit := pgshield.PerSecond(10)
//
//	result, err := AllowN(ctx, "user_1234", limit, 3)
//	if err != nil {
//	    log.Fatalf("Failed to check rate limit: %v", err)
//	}
//
//	if result.Allowed > 0 {
//	    log.Printf("Allowed %d requests, %d remaining.", result.Allowed, result.Remaining)
//	} else {
//	    log.Printf("Rate limit exceeded. Retry after %v, reset after %v.", result.RetryAfter, result.ResetAfter)
//	}
func (l *Limiter) AllowN(
	ctx context.Context,
	key string,
	limit Limit,
	n int,
) (*Result, error) {
	values := []interface{}{keyWithPrefix(l.Options.KeyPrefix, key), limit.Burst, limit.Rate, limit.Period.Seconds(), n}

	row := l.client.QueryRow(ctx, allowN, values...)

	var allowed int
	var remaining, retryAfter, resetAfter float64
	if err := row.Scan(&allowed, &remaining, &retryAfter, &resetAfter); err != nil {
		return nil, err
	}

	res := &Result{
		Limit:      limit,
		Allowed:    allowed,
		Remaining:  int(remaining),
		RetryAfter: dur(retryAfter),
		ResetAfter: dur(resetAfter),
	}

	return res, nil
}

// AllowAtMost attempts to allow up to `n` requests for a given key under a rate-limiting
// scheme defined by the `pgshield.Limit` struct. This method is similar to `AllowN`, but with
// the focus on allowing the maximum possible number of requests up to the limit `n`
// without exceeding the rate limit.
//
// The `pgshield.Limit` struct can be easily created using helper functions like `pgshield.PerSecond`,
// `pgshield.PerMinute`, and `pgshield.PerHour`, which define the rate limit in terms of requests
// per second, minute, or hour respectively. Each helper function sets the `Burst`
// to be equal to the `Rate`, meaning that the system can handle a burst of up to
// `Rate` requests in one period.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the entity being rate-limited (e.g., user ID, IP address).
//   - limit: A `pgshield.Limit` struct defining the rate (requests per period) and burst (maximum burst size).
//     This can be created using helper functions such as `pgshield.PerSecond`, `pgshield.PerMinute`, or `pgshield.PerHour`.
//   - n: The maximum number of requests to attempt to allow.
//
// Returns:
//   - *Result: A struct containing information about the rate limiting state, such as the number
//     of requests actually allowed, remaining, and times for retry and reset.
//   - error: If an error occurs while executing the Lua script or parsing the result, it is returned.
//
// Example:
//
//	// Create a rate limit of 10 requests per minute
//	limit := pgshield.PerMinute(10)
//
//	// Attempt to allow up to 5 requests
//	result, err := AllowAtMost(ctx, "user_1234", limit, 5)
//	if err != nil {
//	    log.Fatalf("Failed to check rate limit: %v", err)
//	}
//
//	log.Printf("Allowed %d requests, %d remaining.", result.Allowed, result.Remaining)
//	if result.Allowed < 5 {
//	    log.Printf("Rate limit exceeded for some requests. Retry after %v, reset after %v.", result.RetryAfter, result.ResetAfter)
//	}
func (l *Limiter) AllowAtMost(
	ctx context.Context,
	key string,
	limit Limit,
	n int,
) (*Result, error) {
	values := []interface{}{keyWithPrefix(l.Options.KeyPrefix, key), limit.Burst, limit.Rate, limit.Period.Seconds(), n}

	row := l.client.QueryRow(ctx, allowAtMost, values...)

	var allowed int
	var remaining, retryAfter, resetAfter float64
	if err := row.Scan(&allowed, &remaining, &retryAfter, &resetAfter); err != nil {
		return nil, err
	}

	res := &Result{
		Limit:      limit,
		Allowed:    allowed,
		Remaining:  int(remaining),
		RetryAfter: dur(retryAfter),
		ResetAfter: dur(resetAfter),
	}
	return res, nil
}

// Reset clears the rate limiter's state for a given key, effectively resetting
// any rate limiting associated with that key. This can be useful if you want to
// manually clear the rate limits for a specific user or action, for example,
// after a penalty period has passed or after a successful manual intervention.
//
// The method removes the key from the underlying storage, which
// effectively resets the rate limiting data (e.g., the number of requests made
// and the timestamps) for that key.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the entity whose rate limit is to be reset.
//
// Returns:
//   - error: If an error occurs during the deletion process, it is returned.
//     If the operation is successful, it returns nil.
//
// Example:
//
//	// Reset the rate limit for user ID 1234
//	err := Reset(ctx, "user_1234")
//	if err != nil {
//		log.Printf("Warning: Failed to reset rate limiter for user_1234: %v", err)
//	}
func (l *Limiter) Reset(ctx context.Context, key string) error {
	_, err := l.client.Exec(ctx, deleteRateLimitByKey, keyWithPrefix(l.Options.KeyPrefix, key))
	if err != nil {
		return err
	}

	return nil
}

// initializeSchema sets up the database schema and functions needed for rate limiting
// inside a transaction to ensure atomic execution. It executes the SQL statements to
// create the `rate_limit` table and necessary functions.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//
// Returns:
//   - error: If an error occurs during execution, it returns the error.
func (l *Limiter) initializeSchema(ctx context.Context) error {
	schemaStatements := []string{
		createTableTokenBucket,
		createFunctionAllowN,
		createFunctionAllowAtMost,
	}

	tx, err := l.client.Begin(ctx)
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

// startCleanup begins a background goroutine that periodically executes
// the cleanup query to remove expired rate limit entries from the database.
// The cleanup interval specifies how often the expired entries should be purged.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - interval: The duration between each cleanup execution.
//
// The cleanup goroutine runs indefinitely until the context is cancelled.
// Each cleanup cycle involves executing the DELETE query to remove entries
// with an expiration timestamp earlier than the current time.
func (l *Limiter) startCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := l.client.Exec(ctx, deleteRateLimitByExpiry); err != nil {
				log.Printf("Error during rate limit cleanup: %v", err)
			} else {
				log.Println("Rate limit cleanup completed successfully")
			}
		case <-ctx.Done():
			log.Println("Stopping rate limit cleanup due to context cancellation")
			return
		}
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
