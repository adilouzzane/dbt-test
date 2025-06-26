with aliens as (
    select * from {{ ref('stg_ben10__aliens') }}
),

enemies as (
    select * from {{ ref('stg_ben10__enemies') }}
),

battles as (
    select
        alien_name,
        min(battle_date) as fist_battle_date,
        max(battle_date) as last_battle_date,
        count(1) as total_battles,
        sum(CASE WHEN alien_name = winner THEN 1 ELSE 0 END) AS won_battles,
        count(distinct enemy_name) AS fought_emenies,
        CASE WHEN alien_name = winner THEN true ELSE false END AS is_winner
    from 
        {{ ref('stg_ben10__battles') }}
    group by
        alien_name,
        winner
),




final AS (
    select
        alien_id,
        alien_name,
        fist_battle_date,
        last_battle_date,
        cast(total_battles as integer),
        cast(won_battles as integer),
        cast(fought_emenies as integer),
        is_winner,
        cast(strength_level as integer) as strength_level,
        cast(speed_level as integer) as speed_level,
        cast(intelligence as integer) as intelligence
    from
        aliens
    left join battles using (alien_name)
    left join enemies using (alien_name)

)

select * from final