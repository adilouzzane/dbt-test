select 
    cast("ID" as integer) as order_id,
    cast("USER_ID" as integer) as customer_id,
    cast("ORDER_DATE" as date) as order_date,
    "STATUS" as order_status
from {{ source('jaffle_shop', 'orders') }}