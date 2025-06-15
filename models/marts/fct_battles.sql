{{
    config(
        materialized='incremental',
        unique_key = 'battle_id',
        incremental_strategy = 'merge',
        on_schema_change = 'fail'
    )
}}

select * from {{ ref('stg_ben10__battles') }} 
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where battle_id > (select max(battle_id) from {{ this }}) 
{% endif %}


-- SELECT * FROM information_schema.columns where table_schema = 'dbt_adilouzzane' and table_name = 'stg_ben10__battles'