select 
    cast("ID" as integer) as payment_id,
    cast("ORDERID" as integer) as order_id,
    "PAYMENTMETHOD" as payment_method,
    "STATUS" as payment_status,
    cast("AMOUNT" as integer) as payment_amount,
    cast("CREATED" as date) as payment_creation_date

from {{ source('stripe', 'payment') }}
