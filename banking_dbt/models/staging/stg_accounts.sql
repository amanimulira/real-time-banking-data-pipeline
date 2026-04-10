{{ config(materialized='view')}}

WITH ranked AS (
    SELECT
    v:id::STRING AS account_id,
    v:customer_id::STRING as customer_id,
    v:account_type::STRING as account_type,
    v:balance::FLOAT as balance,
    v:currency::STRING as currency,
    v:created_at::TIMESTAMP as created_at,
    current_timestamp as load_timestamp,
    ROW_NUMBER() OVER (
        PARTITION BY v:id::STRING
        ORDER BY v:created_at DESC
    ) AS rn 
    FROM {{ source('BANKING_RAW', 'ACCOUNTS')}}
)

SELECT
account_id,
customer_id,
account_type,
balance,
currency,
created_at,
load_timestamp
FROM ranked
WHERE rn = 1