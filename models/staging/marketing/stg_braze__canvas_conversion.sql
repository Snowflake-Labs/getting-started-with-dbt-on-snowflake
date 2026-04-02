select
    id as event_id,
    user_id,
    external_user_id as contact_id,
    cast(null as varchar) as campaign_id,
    canvas_variation_api_id as variation_api_id,
    canvas_id,
    canvas_step_api_id,
    conversion_behavior_index,
    1 as conversion,
    to_timestamp_ntz(time) as event_time_utc,
    cast(to_timestamp_ntz(time) as date) as event_date_utc,
    'CANVAS' as source_type,
    current_timestamp() as load_timestamp
from {{ source('braze_datalake', 'USERS_CANVAS_CONVERSION_SHARED') }}
where cast(to_timestamp_ntz(time) as date) > '2025-12-01'