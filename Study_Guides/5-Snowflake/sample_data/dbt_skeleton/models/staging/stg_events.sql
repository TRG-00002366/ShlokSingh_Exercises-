{{
    config(
        materialized='view'
    )
}}

-- Staging model for raw events
-- Transforms bronze layer data into silver layer (cleansed, typed)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_events') }}
),

renamed AS (
    SELECT
        -- Primary key
        event_id,
        
        -- Normalize event type to uppercase
        UPPER(event_type) AS event_type,
        
        -- Extract fields from VARIANT/JSON payload
        payload:user::STRING AS user_id,
        payload:page::STRING AS page_url,
        payload:product::STRING AS product_id,
        payload:amount::DECIMAL(10,2) AS amount,
        
        -- Keep original timestamp
        created_at AS event_timestamp,
        
        -- Add processing metadata
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
    WHERE event_id IS NOT NULL
)

SELECT * FROM renamed
