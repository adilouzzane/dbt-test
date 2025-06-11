with aliens as (
    select * from {{ ref('stg_ben10__aliens') }}
),

enemies as (
    select * from {{ ref('stg_ben10__enemies') }}
),

battles as (
    select * from {{ ref('stg_ben10__battles') }}
),

final AS (
    select
        *
    from
        aliens
    left join battles using (alien_name)
    left join enemies using (alien_name)

)

select * from final