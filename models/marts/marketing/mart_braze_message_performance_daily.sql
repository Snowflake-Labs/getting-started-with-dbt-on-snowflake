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