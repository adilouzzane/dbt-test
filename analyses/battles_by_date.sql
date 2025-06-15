with battles AS (
    select * from {{ ref('stg_ben10__battles') }}
),

daily as (
    select
        battle_date,
        -- EXTRACT(WEEK FROM battle_date) as battle_week,
        -- EXTRACT(MONTH FROM battle_date) as battle_month,
        -- EXTRACT(YEAR FROM battle_date) as battle_year,
        count(1)
    from
        battles
    group by
        battle_date
)

select * from daily