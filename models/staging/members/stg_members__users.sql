with source as (
    select * from {{ source('members', 'users') }}
),

renamed as (
    select
        user_id,
        email,
        created_at,
        updated_at,
        is_active
    from source
)

select * from renamed
