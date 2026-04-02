-- Session-level metrics enriched with exercise detail
with sessions as (
    select * from {{ ref('stg_workouts__sessions') }}
),

exercises as (
    select
        session_id,
        count(*) as exercise_count,
        sum(sets * reps) as total_reps,
        sum(weight_kg) as total_volume_kg
    from {{ ref('stg_workouts__exercises') }}
    group by 1
),

joined as (
    select
        s.session_id,
        s.user_id,
        s.program_id,
        s.started_at,
        s.completed_at,
        s.duration_seconds,
        s.calories_burned,
        s.is_completed,
        coalesce(e.exercise_count, 0) as exercise_count,
        coalesce(e.total_reps, 0) as total_reps,
        coalesce(e.total_volume_kg, 0) as total_volume_kg
    from sessions s
    left join exercises e on s.session_id = e.session_id
)

select * from joined
