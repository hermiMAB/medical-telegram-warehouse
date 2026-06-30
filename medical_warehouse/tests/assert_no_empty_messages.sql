-- =============================================================================
-- assert_no_empty_messages.sql
-- Custom test: ensures no empty or whitespace-only messages reached the
-- fact table. These should have been filtered out in the staging model.
--
-- This query MUST return 0 rows to pass.
-- =============================================================================

SELECT
    message_id,
    channel_name,
    message_length
FROM {{ ref('fct_messages') }}
WHERE
    message_text IS NULL
    OR TRIM(message_text) = ''
    OR message_length = 0