-- =============================================================================
-- assert_positive_views.sql
-- Custom test: ensures all view counts are non-negative.
--
-- View counts are always >= 0 on Telegram. A negative value means something
-- went wrong during scraping or type casting.
--
-- This query MUST return 0 rows to pass.
-- =============================================================================

SELECT
    message_id,
    channel_name,
    view_count
FROM {{ ref('fct_messages') }}
WHERE view_count < 0