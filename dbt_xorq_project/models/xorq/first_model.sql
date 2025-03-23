{{ config(
    materialized='table'
) }}

-- Read from the source table
SELECT 
    id,
    value,
    current_localtimestamp() as processed_at
FROM {{ source('xorq_flight', 'concurrent_test') }}
