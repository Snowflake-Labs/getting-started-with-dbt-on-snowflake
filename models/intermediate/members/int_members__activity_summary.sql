-- Combines workout sessions + food logs per member per day
with workout_sessions as (
    select
        user_id,
        date_trunc('day', started_at) as activity_date,
        count(*) as workout_count,
        sum(duration_seconds) as total_workout_seconds,
        sum(calories_burned) as total_calories_burned
    from {{ ref('stg_workouts__sessions') }}
    where is_completed = true
    group by 1, 2
),

food_logs as (
    select
        user_id,
        date_trunc('day', logged_at) as activity_date,
        sum(calories) as total_calories_consumed,
        sum(protein_g) as total_protein_g
    from {{ ref('stg_nutrition__food_logs') }}
    group by 1, 2
),

joined as (
    select
        coalesce(w.user_id, f.user_id) as user_id,
        coalesce(w.activity_date, f.activity_date) as activity_date,
        coalesce(w.workout_count, 0) as workout_count,
        coalesce(w.total_workout_seconds, 0) as total_workout_seconds,
        coalesce(w.total_calories_burned, 0) as calories_burned,
        coalesce(f.total_calories_consumed, 0) as calories_consumed,
        coalesce(f.total_protein_g, 0) as protein_g
    from workout_sessions w
    full outer join food_logs f
        on w.user_id = f.user_id
        and w.activity_date = f.activity_date
)

select * from joined
