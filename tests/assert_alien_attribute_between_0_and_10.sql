-- strengh, speed and intellignce must be between 0 and 1
-- Therefore return records where this isn't true to make the test fail.
select 1
from {{ ref('stg_ben10__aliens') }}
where 
    speed_level::int < 0 OR speed_level::int > 10
    OR strength_level::int < 0 OR strength_level::int > 10
    OR intelligence::int < 0 OR intelligence::int > 10