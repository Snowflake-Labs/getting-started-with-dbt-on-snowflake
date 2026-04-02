select
    date_trunc('day', created_at) as post_date,
    count(*) as total_posts,
    count(distinct user_id) as active_posters,
    sum(like_count) as total_likes,
    sum(comment_count) as total_comments
from {{ ref('fct_social_events') }}
where is_deleted = false
group by 1
order by 1
