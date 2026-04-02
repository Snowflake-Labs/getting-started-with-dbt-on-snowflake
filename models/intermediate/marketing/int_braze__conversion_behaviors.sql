with base as (

    select
        source_type,
        source_id,
        campaign_api_id,
        name as campaign_or_canvas_name,
        variation_api_id,
        variation_name,
        event_time_utc,
        conversion_behaviors
    from {{ ref('int_braze__message_metadata_latest') }}

),

flattened as (

    select
        source_type,
        source_id,
        campaign_api_id,
        campaign_or_canvas_name,
        variation_api_id,
        variation_name,
        event_time_utc,
        f.index as conversion_behavior_index,
        f.value:type::string as conversion_type,
        f.value as conversion_behavior_raw
    from base,
    lateral flatten(input => conversion_behaviors) f

)

select
    source_type,
    source_id,
    campaign_api_id,
    campaign_or_canvas_name,
    variation_api_id,
    variation_name,
    event_time_utc,
    conversion_behavior_index,
    conversion_type,
    conversion_behavior_raw
from flattened