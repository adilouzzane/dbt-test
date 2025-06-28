select 
    battle_id,
    alien_name,
    enemy_name,
    TO_DATE(battle_date, 'DD-MM-YYYY') as battle_date,
    winner
from 
    {{ source('ben10', 'ben10_battles') }}