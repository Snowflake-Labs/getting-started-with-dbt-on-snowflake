-- Weekly active members and churn signals
with activity as (
    select * from {{ ref('int_members__activity_summary') }}
),

weekly as (
    select
        user_id,
        date_trunc('week', activity_date) as week_start,
        count(distinct activity_date) as active_days,
        sum(workout_count) as workouts,
        sum(calories_burned) as calories_burned
    from activity
    group by 1, 2
)

select
    week_start,
    count(distinct user_id) as active_members,
    avg(active_days) as avg_active_days,
    avg(workouts) as avg_workouts_per_member,
    sum(calories_burned) as total_calories_burned
from weekly
group by 1
order by 1
