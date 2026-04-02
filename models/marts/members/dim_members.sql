with users as (
    select * from {{ ref('stg_members__users') }}
),

subscriptions as (
    select
        user_id,
        plan_type,
        started_at as subscription_started_at,
        ended_at as subscription_ended_at,
        is_active as has_active_subscription,
        billing_cycle
    from {{ ref('stg_members__subscriptions') }}
    where is_active = true
    qualify row_number() over (partition by user_id order by started_at desc) = 1
)

select
    u.user_id,
    u.email,
    u.created_at,
    u.is_active,
    s.plan_type,
    s.subscription_started_at,
    s.billing_cycle,
    s.has_active_subscription
from users u
left join subscriptions s on u.user_id = s.user_id
