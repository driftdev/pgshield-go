package tokenbucket

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
