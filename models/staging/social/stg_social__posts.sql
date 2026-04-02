with source as (
    select * from {{ source('social', 'posts') }}
),

renamed as (
    select
        post_id,
        user_id,
        post_type,
        created_at,
        like_count,
        comment_count,
        is_deleted
    from source
)

select * from renamed
