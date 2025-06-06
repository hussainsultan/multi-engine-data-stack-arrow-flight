{{ config(
    materialized='table',
    adapter_plugin='flight',
    target= "iceberg"
) }}

-- Read from the source table
SELECT 
    id,
    value,
    current_localtimestamp() as processed_at
FROM {{ source('xorq_flight', 'concurrent_test') }}
