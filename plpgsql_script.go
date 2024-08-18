package pgshield

const createTableTokenBucket = `
create table if not exists rate_limit  (
    key text primary key,
    tat float not null,
    expires_at timestamptz null
);

create index if not exists rate_limit_key_ix ON rate_limit (key);
`

const createFunctionAllowN = `
create or replace function allow_n(
    rate_limit_key text,
    burst integer,
    rate integer,
    period float,
    cost integer
)
returns table (
    allowed integer,
    remaining float,
    retry_after float,
    reset_after float
) language plpgsql as $$
declare
    emission_interval float := period / rate;
    increment float := emission_interval * cost;
    burst_offset float := emission_interval * burst;
    jan_1_2017 timestamptz := '2017-01-01 00:00:00 utc';
    now float := extract(epoch from current_timestamp - jan_1_2017);
    tat float;
    new_tat float;
    allow_at float;
    diff float;
    reset_after_time float;
    retry_after_time float;
    remaining_requests float;
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

        return query select 0, 0::float, retry_after_time, reset_after_time;
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
    rate integer,
    period float,
    cost integer
)
returns table (
    allowed integer,
    remaining float,
    retry_after float,
    reset_after float
) language plpgsql as $$
declare
    emission_interval float := period / rate;
    burst_offset float := emission_interval * burst;
    jan_1_2017 timestamptz := '2017-01-01 00:00:00 utc';
    now float := extract(epoch from current_timestamp - jan_1_2017);
    tat float;
    diff float;
    remaining_requests float;
    new_tat float;
    reset_after_time float;
    retry_after_time float;
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

        return query select 0, 0::float, retry_after_time, reset_after_time limit 1;
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

    return query select cost, remaining_requests, (-1)::float, reset_after_time;
end;
$$;
`
const deleteRateLimitByExpiry = `delete from rate_limit where expires_at < now() limit $1;`

const allowN = `select allowed, remaining, retry_after, reset_after from allow_n($1, $2, $3, $4, $5) limit 1`

const allowAtMost = `select allowed, remaining, retry_after, reset_after from allow_at_most($1, $2, $3, $4, $5) limit 1`

const deleteRateLimitByKey = `delete from rate_limit where key = $1`
