-- audience selection SQL for experiment
-- Replace or edit this file to define the audience.
SELECT user_id
FROM {{ source('analytics','users') }}
WHERE is_active = TRUE
LIMIT 100000
