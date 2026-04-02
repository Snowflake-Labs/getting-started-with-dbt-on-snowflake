select
    post_id,
    user_id,
    post_type,
    created_at,
    like_count,
    comment_count,
    is_deleted
from {{ ref('stg_social__posts') }}
