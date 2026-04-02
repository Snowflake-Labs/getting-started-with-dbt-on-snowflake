select
    id as source_id,
    api_id as canvas_step_api_id,
    name as step_name,
    to_timestamp_ntz(time) as event_time_utc,
    app_group_id
from {{ source('braze_datalake', 'SNAPSHOTS_CANVAS_STEP_SHARED') }}