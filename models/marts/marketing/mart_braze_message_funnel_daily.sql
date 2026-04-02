with event_metrics as (

    select
        event_date_utc,
        variation_api_id,
        canvas_step_api_id,
        channel,

        count_if(event_type = 'SEND') as messages_sent,
        count_if(event_type = 'OPEN') as messages_opened,
        count_if(event_type = 'CLICK') as messages_clicked,
        count_if(event_type = 'IMPRESSION') as impressions,
        count_if(event_type = 'INFLUENCED_OPEN') as influenced_opens,
        count_if(event_type = 'ABORT') as aborts,
        count_if(event_type = 'BOUNCE') as bounces,
        count_if(event_type = 'RETRY') as retries,

        count(distinct case when event_type = 'SEND' then contact_id end) as unique_recipients,
        count(distinct case when event_type = 'OPEN' then contact_id end) as unique_openers,
        count(distinct case when event_type = 'CLICK' then contact_id end) as unique_clickers

    from {{ ref('fct_braze_message_events') }}
    group by 1,2,3,4

),



conversion_metrics as (

    select
        event_date_utc,
        variation_api_id,
        canvas_step_api_id,
        sum(conversion) as conversions,
        count(distinct case when conversion = 1 then contact_id end) as unique_converters
    from {{ ref('fct_braze_conversion') }}
    group by 1,2,3

)

select
    e.event_date_utc,
    e.variation_api_id,
    e.canvas_step_api_id,
    e.channel,

    m.source_type,
    m.source_id,
    m.campaign_api_id,
    m.campaign_or_canvas_name,
    m.variation_name,

    s.step_name,

    e.messages_sent,
    e.messages_opened,
    e.messages_clicked,
    e.impressions,
    e.influenced_opens,
    e.aborts,
    e.bounces,
    e.retries,
    e.unique_recipients,
    e.unique_openers,
    e.unique_clickers,

    coalesce(c.conversions, 0) as conversions,
    coalesce(c.unique_converters, 0) as unique_converters

from event_metrics e
left join {{ ref('dim_braze_message_metadata') }} m
    on e.variation_api_id = m.variation_api_id
left join {{ ref('dim_braze_canvas_step') }} s
    on e.canvas_step_api_id = s.canvas_step_api_id
left join conversion_metrics c
    on e.event_date_utc = c.event_date_utc
   and e.variation_api_id = c.variation_api_id
   and coalesce(e.canvas_step_api_id, '') = coalesce(c.canvas_step_api_id, '')