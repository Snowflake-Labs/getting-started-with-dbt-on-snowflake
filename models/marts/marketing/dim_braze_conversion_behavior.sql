select
    source_type,
    source_id,
    campaign_api_id,
    campaign_or_canvas_name,
    variation_api_id,
    variation_name,
    conversion_behavior_index,
    conversion_type,
    conversion_behavior_raw,
    event_time_utc
from {{ ref('int_braze__conversion_behaviors') }}