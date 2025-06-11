select 
    alien_name,
    count(enemy_name) as total_enemies
from 
    {{ source('ben10', 'ben10_enemies') }}
group by
    alien_name