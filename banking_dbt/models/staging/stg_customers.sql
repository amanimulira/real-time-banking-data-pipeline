{{ config(materialized='view')}}

WITH ranked AS (
    SELECT
    v:id::STRING AS customer_id,
    v:first_name::STRING as first_name,
    v:last_name::STRING as last_name,
    v:email::STRING as email,
    v:created_at::TIMESTAMP as created_at,
    current_timestamp as load_timestamp,
    ROW_NUMBER() OVER (
        PARTITION BY v:id::STRING
        ORDER BY v:created_at DESC
    ) AS rn 
    FROM {{ source('BANKING_RAW', 'CUSTOMERS')}}
)

SELECT
customer_id,
first_name, 
last_name,
email,
created_at,
load_timestamp
FROM ranked
WHERE rn = 1