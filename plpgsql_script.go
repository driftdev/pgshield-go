package pgshield

const createTableTokenBucket = `
create table if not exists rate_limit  (
    key text primary key,
    tat double precision not null,
    expires_at timestamptz null
);

create index if not exists rate_limit_key_ix ON rate_limit (key);
`

const createFunctionAllowN = `
create or replace function allow_n(
    rate_limit_key text,
    burst integer,
    rate double precision,
    period double precision,
    cost integer
)
returns table (
    allowed integer,
    remaining double precision,
    retry_after double precision,
    reset_after double precision
) language plpgsql as $$
declare
    emission_interval double precision := period / rate;
    increment double precision := emission_interval * cost;
    burst_offset double precision := emission_interval * burst;
    jan_1_2017 timestamptz := '2017-01-01 00:00:00 utc';
    now double precision := extract(epoch from current_timestamp - jan_1_2017);
    tat double precision;
    new_tat double precision;
    allow_at double precision;
    diff double precision;
    reset_after_time double precision;
    retry_after_time double precision;
    remaining_requests double precision;
begin
    select rl.tat into tat
    from rate_limit rl
    where rl.key = rate_limit_key
    for update;

    if not found then
        tat := now;
    end if;

    tat := greatest(tat, now);
    new_tat := tat + increment;
    allow_at := new_tat - burst_offset;
    diff := now - allow_at;
    remaining_requests := diff / emission_interval;

    if remaining_requests < 0 then
        reset_after_time := tat - now;
        retry_after_time := -diff;

        return query select 0, 0::double precision, retry_after_time, reset_after_time;
    end if;

    reset_after_time := new_tat - now;

    if reset_after_time > 0 then
        insert into rate_limit(key, tat, expires_at)
        values (rate_limit_key, new_tat, now() + interval '1 second' * ceil(reset_after_time))
        on conflict (key) do update
        set tat = excluded.tat,
            expires_at = excluded.expires_at;
    end if;

    retry_after_time := -1;
    return query select cost, remaining_requests, retry_after_time, reset_after_time;
end;
$$;
`

const createFunctionAllowAtMost = `
create or replace function allow_at_most(
    rate_limit_key text,
    burst integer,
    rate double precision,
    period double precision,
    cost integer
)
returns table (
    allowed integer,
    remaining double precision,
    retry_after double precision,
    reset_after double precision
) language plpgsql as $$
declare
    emission_interval double precision := period / rate;
    burst_offset double precision := emission_interval * burst;
    jan_1_2017 timestamptz := '2017-01-01 00:00:00 utc';
    now double precision := extract(epoch from current_timestamp - jan_1_2017);
    tat double precision;
    diff double precision;
    remaining_requests double precision;
    new_tat double precision;
    reset_after_time double precision;
    retry_after_time double precision;
begin
    select rl.tat into tat
    from rate_limit rl
    where rl.key = rate_limit_key
    for update;

    if not found then
        tat := now;
    end if;

    tat := greatest(tat, now);

    diff := now - (tat - burst_offset);
    remaining_requests := diff / emission_interval;

    if remaining_requests < 1 then
        reset_after_time := tat - now;
        retry_after_time := emission_interval - diff;

        return query select 0, 0::double precision, retry_after_time, reset_after_time limit 1;
    end if;

    if remaining_requests < cost then
        cost := remaining_requests;
        remaining_requests := 0;
    else
        remaining_requests := remaining_requests - cost;
    end if;

    new_tat := tat + emission_interval * cost;
    reset_after_time := new_tat - now;

    if reset_after_time > 0 then
        insert into rate_limit(key, tat, expires_at)
        values (rate_limit_key, new_tat, now() + interval '1 second' * reset_after_time)
        on conflict (key) do update set tat = excluded.tat, expires_at = excluded.expires_at;
    end if;

    return query select cost, remaining_requests, (-1)::double precision, reset_after_time;
end;
$$;
`
const deleteRateLimitByExpiry = `delete from rate_limit where expires_at < now() limit $1;`

const allowN = `select allowed, remaining, retry_after, reset_after from allow_n($1, $2, $3, $4, $5) limit 1`

const allowAtMost = `select allowed, remaining, retry_after, reset_after from allow_at_most($1, $2, $3, $4, $5) limit 1`

const deleteRateLimitByKey = `delete from rate_limit where key = $1`
