/*
I tested this query on Bigquery because I was unable to run the provided
docker-compose file on my personal laptop due to ram limitations.
*/
with ranked_indexes as (
    select
    t.transaction_id,
    t.account_id,
    t.client_id,
    t.currency_1,
    t.currency_2,
    t.exchange,
    t.exchange_type,
    t.executed_at,
    i.value,
    i.updated_at,
    ROW_NUMBER() OVER (PARTITION BY t.currency_1, t.currency_2, t.exchange, t.exchange_type ORDER BY ABS(TIMESTAMP_DIFF(t.executed_at, i.updzted_at, SECOND))) AS rn
    FROM
        trades t
    JOIN
        indexes i ON t.exchange = i.exchange
                   AND t.exchange_type = i.exchange_type
)
SELECT
    transaction_id,
    exchange,
    exchange_type,
    account_id,
    client_id,
    currency_1,
    currency_2,
    value,
    updated_at
FROM
    ranked_indexes
WHERE
    rn = 1;
