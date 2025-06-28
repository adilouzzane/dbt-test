-- Import CTE
with orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
),

customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

payment as (
    select * from {{ source('stripe', 'payment') }}
),

-- Logic CTE

success_paied_orders as (
    select
        "ORDERID" as order_id,
        max("CREATED") as payment_finalized_date,
        sum(cast("AMOUNT" as integer)) / 100.0 as total_amount_paid
    from payment
    where "STATUS" <> 'fail'
    group by 1
),

paid_orders as (
    select
        orders."ID" as order_id,
        orders."USER_ID" as customer_id,
        orders."ORDER_DATE" as order_placed_at,
        orders."STATUS" as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c."FIRST_NAME" as customer_first_name,
        c."LAST_NAME" as customer_last_name
    from orders
    left join success_paied_orders p on orders."ID" = p.order_id
    left join customers c on orders."USER_ID" = c."ID"
),

customer_orders as (
    select
        c."ID" as customer_id,
        min("ORDER_DATE") as first_order_date,
        max("ORDER_DATE") as most_recent_order_date,
        count(orders."ID") as number_of_orders
    from customers c
    left join orders on orders."USER_ID" = c."ID"
    group by 1
),

cumulative_paid_orders as (
    select 
        p.order_id, 
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
),

-- Final CTE
final as (
select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by p.order_id
    ) as customer_sales_seq,
    case
        when c.first_order_date = p.order_placed_at then 'new' else 'return'
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join cumulative_paid_orders x on x.order_id = p.order_id
order by order_id
)

select * from final
