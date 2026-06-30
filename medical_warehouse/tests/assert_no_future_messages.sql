-- =============================================================================
-- assert_no_future_messages.sql
-- Custom test: ensures no messages have a date in the future.
--
-- A Telegram message cannot be posted in the future. Any such record
-- indicates a data quality issue in the scraper or a timezone conversion bug.
--
-- This query MUST return 0 rows to pass.
-- =============================================================================

SELECT
    message_id,
    channel_name,
    message_date,
    NOW() AS current_time
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > NOW()