-- Import CTE
with orders as (
    select
        "ID" as order_id,
        "USER_ID" as customer_id,
        "ORDER_DATE" as order_date,
        "STATUS" as order_status
    from {{ source('jaffle_shop', 'orders') }}
),

customers as (
    select
        "ID" as customer_id,
        "FIRST_NAME" as first_name,
        "LAST_NAME" as last_name
    from {{ source('jaffle_shop', 'customers') }}
),

success_payment as (
    select
        "ORDERID" as order_id,
        "CREATED" as payment_creation_date,
        cast("AMOUNT" as integer) as payment_amount
    from {{ source('stripe', 'payment') }}
    where "STATUS" <> 'fail'
),

-- Logic CTE

success_paid_orders as (
    select
        order_id,
        max(payment_creation_date) as payment_finalized_date,
        sum(payment_amount) / 100.0 as total_amount_paid
    from success_payment
    group by 1
),

paid_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.order_status,
        total_amount_paid,
        payment_finalized_date,
        first_name,
        last_name
    from orders
    left join success_paid_orders on orders.order_id = success_paid_orders.order_id
    left join customers on orders.customer_id = customers.customer_id
),

customer_orders as (
    select
        customers.customer_id as customer_id,
        min("order_date") as first_order_date,
        max("order_date") as most_recent_order_date,
        count(order_id) as number_of_orders
    from customers
    left join orders on orders.customer_id = customers.customer_id
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
        when c.first_order_date = p.order_date then 'new' else 'return'
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join cumulative_paid_orders x on x.order_id = p.order_id
order by order_id
)

select * from final
