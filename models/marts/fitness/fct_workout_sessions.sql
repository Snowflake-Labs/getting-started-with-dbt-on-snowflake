select
    session_id,
    user_id,
    program_id,
    started_at,
    completed_at,
    duration_seconds,
    calories_burned,
    is_completed,
    exercise_count,
    total_reps,
    total_volume_kg
from {{ ref('int_workouts__session_metrics') }}
