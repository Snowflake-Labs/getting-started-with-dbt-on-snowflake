select
    date_trunc('day', started_at) as workout_date,
    count(*) as total_sessions,
    count(distinct user_id) as unique_members,
    avg(duration_seconds) as avg_duration_seconds,
    sum(calories_burned) as total_calories_burned,
    avg(exercise_count) as avg_exercises_per_session,
    avg(total_volume_kg) as avg_volume_kg
from {{ ref('fct_workout_sessions') }}
where is_completed = true
group by 1
order by 1
