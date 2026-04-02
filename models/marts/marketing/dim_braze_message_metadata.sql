select
    source_type,
    source_id,
   campaign_api_id,
    name as campaign_or_canvas_name,
    variation_api_id,
    variation_name,
    variation_id,
    conversion_behaviors,
    event_time_utc
from {{ ref('int_braze__message_metadata_latest') }}