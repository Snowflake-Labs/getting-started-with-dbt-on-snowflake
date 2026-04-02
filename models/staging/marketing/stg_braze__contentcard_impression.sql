select
    id as event_id,
    user_id,
    external_user_id as contact_id,
    null as email_address,
    country,
    campaign_id,
    canvas_id,
    coalesce(message_variation_api_id, canvas_variation_api_id) as variation_api_id,
    canvas_step_message_variation_api_id,
    canvas_step_api_id,
    content_card_id,
    'CONTENTCARD' as channel,
    'IMPRESSION' as event_type,
    to_timestamp_ntz(time) as event_time_utc,
    cast(to_timestamp_ntz(time) as date) as event_date_utc,
    current_timestamp() as load_timestamp
from {{ source('braze_datalake', 'USERS_MESSAGES_CONTENTCARD_IMPRESSION_SHARED') }}
where cast(to_timestamp_ntz(time) as date) > '2025-12-01'