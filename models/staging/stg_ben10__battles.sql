select
    alien_name,
    min(battle_date) as fist_battle_date,
    max(battle_date) as last_battle_date,
    count(1) as total_battles,
    sum(CASE WHEN alien_name = winner THEN 1 ELSE 0 END) AS won_battles,
    count(distinct enemy_name) AS fought_emenies
from 
    {{ source('ben10', 'ben10_battles') }}
group by
    alien_name