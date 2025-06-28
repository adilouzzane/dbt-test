with orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_status
    from {{ ref('stg_jaffle_shop__orders') }}
),

customers as (
    select
        customer_id,
        first_name,
        last_name
    from {{ ref('stg_jaffle_shop__customers') }}
),


paid_orders as (
    select * from {{ ref('int_orders_paiment') }}
),

-- Can be removed by implementing window function in the final CTE
customer_orders as (
    select
        customers.customer_id as customer_id,
        first_name,
        last_name,
        min(orders.order_date) as first_order_date
    from customers
    left join orders on orders.customer_id = customers.customer_id
    group by 1,2,3
),

cumulative_paid_orders as (
    select 
        p.order_id, 
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
),

-- Final CTE
final as (
select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    first_name,
    last_name,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by paid_orders.order_id
    ) as customer_sales_seq,
    case
        when customer_orders.first_order_date = paid_orders.order_date then 'new' else 'return'
    end as nvsr,
    cumulative_paid_orders.clv_bad as customer_lifetime_value,
    customer_orders.first_order_date as fdos
from paid_orders
left join customer_orders using (customer_id)
left outer join cumulative_paid_orders using (order_id)
order by order_id
)

select * from final
