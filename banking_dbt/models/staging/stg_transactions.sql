{{ config(materialized='view')}}

SELECT
v:id::STRING AS transaction_id,
v:account_id::STRING AS account_id,
v:amount::FLOAT AS amount,
v:txn_type::STRING AS transaction_type,
v:related_account_id::STRING AS related_account_id,
v:status::STRING AS status,
v:created_at::TIMESTAMP AS transaction_time,
CURRENT_TIMESTAMP AS load_timestamp
FROM {{ source('BANKING_RAW', 'TRANSACTIONS')}}