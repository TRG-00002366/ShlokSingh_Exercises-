{{
    config(
        materialized='table'
    )
}}

-- Mart model: Daily event counts by type
-- Aggregates staging data into business-ready metrics

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

aggregated AS (
    SELECT
        DATE_TRUNC('day', event_timestamp)::DATE AS event_date,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        SUM(COALESCE(amount, 0)) AS total_amount
    FROM events
    GROUP BY 1, 2
)

SELECT
    event_date,
    event_type,
    event_count,
    unique_users,
    total_amount,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM aggregated
