with campaign_conversion as (

    select
        event_id,
        user_id,
        contact_id,
        campaign_id,
        variation_api_id,
        canvas_id,
        canvas_step_api_id,
        conversion_behavior_index,
        conversion,
        event_time_utc,
        event_date_utc,
        source_type,
        load_timestamp
    from {{ ref('stg_braze__campaign_conversion') }}

),

canvas_conversion as (

    select
        event_id,
        user_id,
        contact_id,
        campaign_id,
        variation_api_id,
        canvas_id,
        canvas_step_api_id,
        conversion_behavior_index,
        conversion,
        event_time_utc,
        event_date_utc,
        source_type,
        load_timestamp
    from {{ ref('stg_braze__canvas_conversion') }}

),

unioned as (

    select * from campaign_conversion
    union all
    select * from canvas_conversion

)

select * from unioned