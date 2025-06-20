
{% set home_planets = ['Vega', 'Tierra Nova', 'Rylos'] %}
{% set default_planet = 'Other' %}

with aliens AS (
    select * from {{ ref('stg_ben10__aliens') }}
),

pivoted as (
    select
        alien_name,
    {% for home in home_planets %}
        sum(case when home_planet = '{{ home }}' then 1 else 0 end) as "{{ home }}_planet" -- home|replace(" ","_")
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {% endfor %}
    from 
        aliens
    group by
        alien_name
)

select * from pivoted