with source as (
    select * from {{ source('members', 'subscriptions') }}
),

renamed as (
    select
        subscription_id,
        user_id,
        plan_type,
        started_at,
        ended_at,
        is_active,
        billing_cycle
    from source
)

select * from renamed
