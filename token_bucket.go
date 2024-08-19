package pgshield

import (
	"context"
	"log"
)

const createTokenBucketTable = `
create table if not exists token_bucket
(
    key       text primary key,
    tokens    bigint      not null,
    timestamp timestamptz not null
);

create index if not exists token_bucket_key_ix on token_bucket (key);
`
const createFunctionConsumeTokens = `
create or replace function consume_tokens(rate_limit_key text, refill_per_sec int, window_seconds int)
    returns int as
$$
declare
    new_tokens int := null;
begin
    update token_bucket
    set timestamp=now(),
        tokens=greatest(least(
                                tokens - 1 + refill_per_sec * extract(epoch from clock_timestamp() - timestamp)
                            , window_seconds * refill_per_sec), -1)
    where token_bucket.key = rate_limit_key
    returning tokens into new_tokens;

    if new_tokens is null then
        insert into token_bucket (key, tokens, timestamp)
        values (rate_limit_key, window_seconds * refill_per_sec - 1, clock_timestamp())
        returning tokens into new_tokens;
    end if;

    return new_tokens;
end;
$$ language plpgsql;
`

const consumeTokens = `select * from consume_tokens($1, $2, $3) limit 1;`

type TokenLimit struct {
	RefillPerSec  int
	WindowSeconds int
}

// PerSecond creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per second with a burst capacity equal to the refill rate.
// Example: PerSecond(5) creates a TokenLimit allowing 5 tokens per second with a burst capacity of 5.
func PerSecond(tokens int) TokenLimit {
	return TokenLimit{
		RefillPerSec:  tokens,
		WindowSeconds: 1,
	}
}

// PerMinute creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per minute with a burst capacity equal to the refill rate.
// Example: PerMinute(300) creates a TokenLimit allowing 300 tokens per minute with a burst capacity of 300.
func PerMinute(tokens int) TokenLimit {
	return TokenLimit{
		RefillPerSec:  tokens / 60,
		WindowSeconds: 60,
	}
}

// PerHour creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per hour with a burst capacity equal to the refill rate.
// Example: PerHour(18000) creates a TokenLimit allowing 18000 tokens per hour with a burst capacity of 18000.
func PerHour(tokens int) TokenLimit {
	return TokenLimit{
		RefillPerSec:  tokens / 3600,
		WindowSeconds: 3600,
	}
}

type TokenResult struct {
	Limit  TokenLimit
	Tokens int
}

type TokenBucketRateLimiter struct {
	client  pgxUniversalClient
	Options Options
}

func NewTokenBucketRateLimiter(client pgxUniversalClient, options Options) *TokenBucketRateLimiter {
	ctx := context.Background()

	limiter := &TokenBucketRateLimiter{
		client:  client,
		Options: options,
	}

	if err := limiter.initializeSchema(ctx); err != nil {
		log.Fatalf("error initializing token bucket rate limiter scheme: %s", err)
	}

	return limiter
}

func (t *TokenBucketRateLimiter) ConsumeTokens(ctx context.Context, key string, limit TokenLimit) (*TokenResult, error) {
	row := t.client.QueryRow(ctx, consumeTokens, keyWithPrefix(t.Options.KeyPrefix, key), limit.RefillPerSec, limit.WindowSeconds)

	var tokens int
	err := row.Scan(&tokens)
	if err != nil {
		return nil, err
	}

	return &TokenResult{
		Limit:  limit,
		Tokens: tokens,
	}, nil
}

func (t *TokenBucketRateLimiter) initializeSchema(ctx context.Context) error {
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
