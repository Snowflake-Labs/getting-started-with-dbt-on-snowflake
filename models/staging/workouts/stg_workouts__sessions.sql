with source as (
    select * from {{ source('workouts', 'sessions') }}
),

renamed as (
    select
        session_id,
        user_id,
        program_id,
        started_at,
        completed_at,
        duration_seconds,
        calories_burned,
        is_completed
    from source
)

select * from renamed
