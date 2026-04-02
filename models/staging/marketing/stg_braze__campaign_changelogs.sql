select
    id as source_id,
    api_id as campaign_api_id,
    name,
    conversion_behaviors,
    actions,
    to_timestamp_ntz(time) as event_time_utc,
    app_group_id
from {{ source('braze_datalake', 'CHANGELOGS_CAMPAIGN_SHARED') }}
