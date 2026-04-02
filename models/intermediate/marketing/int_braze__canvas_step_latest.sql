with ranked as (

    select
        source_id,
        canvas_step_api_id,
        step_name,
        event_time_utc,
        row_number() over (
            partition by canvas_step_api_id
            order by event_time_utc desc
        ) as rn
    from {{ ref('stg_braze__canvas_step_snapshots') }}

)

select
    source_id,
    canvas_step_api_id,
    step_name,
    event_time_utc
from ranked
where rn = 1