with ranked_indexes as (
    select
    t.transaction_id,
    t.currency_1,
    t.currency_2,
    t.exchange,
    t.exchange_type,
    t.executed_at,
    i.value,
    i.updated_at,
    ROW_NUMBER() OVER (PARTITION BY t.currency_1, t.currency_2, t.exchange, t.exchange_type ORDER BY ABS(TIMESTAMPDIFF(SECOND, t.executed_at, i.updzted_at))) AS rn
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
