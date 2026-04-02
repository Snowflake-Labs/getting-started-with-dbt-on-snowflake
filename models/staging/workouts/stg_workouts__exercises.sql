with source as (
    select * from {{ source('workouts', 'exercises') }}
),

renamed as (
    select
        exercise_id,
        session_id,
        exercise_name,
        sets,
        reps,
        weight_kg,
        duration_seconds
    from source
)

select * from renamed
