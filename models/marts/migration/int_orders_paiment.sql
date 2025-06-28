with orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_status
    from {{ ref('stg_jaffle_shop__orders') }}
),

success_payment as (
    select
        order_id,
        payment_creation_date,
        payment_amount
    from {{ ref('stg_stripe__payment') }}
    where payment_status <> 'fail'
),

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
        success_paid_orders.total_amount_paid,
        success_paid_orders.payment_finalized_date

    from orders
    left join success_paid_orders on orders.order_id = success_paid_orders.order_id
)

select * from paid_orders