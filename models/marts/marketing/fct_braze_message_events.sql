with email_send as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        send_id,
        dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        cast(null as varchar) as content_card_id,
        'EMAIL' as channel,
        'SEND' as event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_EMAIL_SEND_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__email_send') }}

),

email_open as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        send_id,
        dispatch_id,
        machine_open,
        cast(null as varchar) as url,
        cast(null as varchar) as content_card_id,
        'EMAIL' as channel,
        'OPEN' as event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_EMAIL_OPEN_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__email_open') }}

),

email_click as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        send_id,
        dispatch_id,
        cast(null as varchar) as machine_open,
        url,
        cast(null as varchar) as content_card_id,
        'EMAIL' as channel,
        'CLICK' as event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_EMAIL_CLICK_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__email_click') }}

),

push_send as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_PUSHNOTIFICATION_SEND_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__pushnotification_send') }}

),

push_influenced_open as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_PUSHNOTIFICATION_INFLUENCEDOPEN_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__pushnotification_influencedopen') }}

),

push_abort as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_PUSHNOTIFICATION_ABORT_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__pushnotification_abort') }}

),

push_bounce as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_PUSHNOTIFICATION_BOUNCE_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__pushnotification_bounce') }}

),

push_retry as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_PUSHNOTIFICATION_RETRY_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__pushnotification_retry') }}

),

contentcard_send as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_CONTENTCARD_SEND_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__contentcard_send') }}

),

contentcard_impression as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_CONTENTCARD_IMPRESSION_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__contentcard_impression') }}

),

contentcard_click as (

    select
        event_id,
        user_id,
        contact_id,
        email_address,
        country,
        campaign_id,
        cast(null as varchar) as campaign_api_id,
        canvas_id,
        variation_api_id,
        canvas_step_message_variation_api_id,
        canvas_step_api_id,
        cast(null as varchar) as send_id,
        cast(null as varchar) as dispatch_id,
        cast(null as varchar) as machine_open,
        cast(null as varchar) as url,
        content_card_id,
        channel,
        event_type,
        event_time_utc,
        event_date_utc,
        'BRAZE' as data_source,
        'USERS_MESSAGES_CONTENTCARD_CLICK_SHARED' as raw_source_table,
        load_timestamp
    from {{ ref('stg_braze__contentcard_click') }}

),

unioned as (

    select * from email_send
    union all
    select * from email_open
    union all
    select * from email_click
    union all
    select * from push_send
    union all
    select * from push_influenced_open
    union all
    select * from push_abort
    union all
    select * from push_bounce
    union all
    select * from push_retry
    union all
    select * from contentcard_send
    union all
    select * from contentcard_impression
    union all
    select * from contentcard_click

)

select
    event_id,
    user_id,
    contact_id,
    email_address,
    country,
    campaign_id,
    campaign_api_id,
    canvas_id,
    variation_api_id,
    canvas_step_message_variation_api_id,
    canvas_step_api_id,
    send_id,
    dispatch_id,
    machine_open,
    url,
    content_card_id,
    channel,
    event_type,
    event_time_utc,
    event_date_utc,
    data_source,
    raw_source_table,
    load_timestamp
from unioned