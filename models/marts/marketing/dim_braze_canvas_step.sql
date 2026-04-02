select
    source_id,
    canvas_step_api_id,
    step_name,
    event_time_utc
from {{ ref('int_braze__canvas_step_latest') }}