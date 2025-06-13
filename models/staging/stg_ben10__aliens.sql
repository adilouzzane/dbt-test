
select 
    alien_id,
    alien_name,
    species,
    home_planet,
    cast(strength_level as integer) as strength_level,
    cast(speed_level as integer) as speed_level,
    cast(intelligence as integer) as intelligence

from {{ source('ben10', 'ben10_aliens') }}


-- testing :

-- select * from ben10_aliens

-- select distinct home_planet from ben10_aliens

-- select 
    -- *
    -- max(cast(strength_level as integer)),
    -- min(cast(strength_level as integer)),
    -- max(cast(speed_level as integer)),
    -- min(cast(speed_level as integer)),
    -- max(cast(intelligence as integer)),
    -- min(cast(intelligence as integer))
-- from ben10_aliens
-- order by strength_level asc