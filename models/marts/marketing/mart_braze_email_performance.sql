with sends as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        coalesce(user_id, contact_id) as recipient_id,
        coalesce(send_id, dispatch_id) as send_or_dispatch_id,
        event_time_utc as send_time
    from {{ ref('stg_braze__email_send') }}
    where coalesce(send_id, dispatch_id) is not null

),

deliveries as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        coalesce(user_id, contact_id) as recipient_id,
        dispatch_id,
        event_time_utc as delivery_time
    from {{ ref('stg_braze__email_delivery') }}
    where dispatch_id is not null

),

opens as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        coalesce(user_id, contact_id) as recipient_id,
        dispatch_id,
        machine_open,
        event_time_utc as open_time
    from {{ ref('stg_braze__email_open') }}
    where dispatch_id is not null

),

clicks as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        coalesce(user_id, contact_id) as recipient_id,
        dispatch_id,
        url,
        event_time_utc as click_time
    from {{ ref('stg_braze__email_click') }}
    where dispatch_id is not null

),

send_metrics as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        count(distinct recipient_id || '|' || send_or_dispatch_id) as sends
    from sends
    group by 1,2,3

),

delivery_metrics as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        count(distinct recipient_id || '|' || dispatch_id) as deliveries
    from deliveries
    group by 1,2,3

),

total_open_metrics as (

    select
        variation_api_id,
        campaign_id,
        campaign_api_id,
        count(*) as total_opens
    from opens
    group by 1,2,3

),

open_metrics_all as (

    select
        d.variation_api_id,
        d.campaign_id,
        d.campaign_api_id,
        count(distinct d.recipient_id) as unique_opens_all
    from deliveries d
    left join opens o
      on d.variation_api_id = o.variation_api_id
     and d.campaign_api_id = o.campaign_api_id
     and d.recipient_id = o.recipient_id
     and d.dispatch_id = o.dispatch_id

    group by 1,2,3

),

click_metrics as (

    select
        d.variation_api_id,
        d.campaign_id,
        d.campaign_api_id,
        count(distinct d.recipient_id) as unique_clicks
    from deliveries d
    left join clicks c
      on d.variation_api_id = c.variation_api_id
     and d.campaign_api_id = c.campaign_api_id
     and d.recipient_id = c.recipient_id
     and d.dispatch_id = c.dispatch_id

    group by 1,2,3

),

unsubscribe_metrics as (

    select
        d.variation_api_id,
        d.campaign_id,
        d.campaign_api_id,
        count(distinct d.recipient_id || '|' || d.dispatch_id) as clicked_unsubscribe
    from deliveries d
    left join clicks c
      on d.variation_api_id = c.variation_api_id
     and d.campaign_api_id = c.campaign_api_id
     and d.recipient_id = c.recipient_id
     and d.dispatch_id = c.dispatch_id
     and lower(c.url) like '%unsubscribe%'
    where c.dispatch_id is not null
    group by 1,2,3

),

conversion_metrics as (

    select
        variation_api_id,
        count(distinct case when conversion_behavior_index = 0 then contact_id end) as conversion_a,
        count(distinct case when conversion_behavior_index = 1 then contact_id end) as conversion_b,
        count(distinct case when conversion_behavior_index = 2 then contact_id end) as conversion_c
    from {{ ref('fct_braze_conversion') }}
    group by 1

)




select
    m.campaign_or_canvas_name as braze_name,
    m.variation_name,
    tag.tags,
    d.campaign_id,
    d.campaign_api_id,
    d.variation_api_id,

    coalesce(s.sends,0) as sends,
    d.deliveries,

    coalesce(t.total_opens,0) as total_opens,


    coalesce(oa.unique_opens_all,0) as unique_opens_all,
    round(coalesce(oa.unique_opens_all,0) / nullif(d.deliveries,0) * 100,2) as open_rate_all_pct,


    coalesce(c.unique_clicks,0) as unique_clicks,
    round(coalesce(c.unique_clicks,0) / nullif(d.deliveries,0) * 100,2) as ctr_pct,

    coalesce(u.clicked_unsubscribe,0) as clicked_unsubscribe,
    round(coalesce(u.clicked_unsubscribe,0) / nullif(d.deliveries,0) * 100,2) as unsubscribe_rate_pct,

    coalesce(conv.conversion_a,0) as conversion_a,
    round(coalesce(conv.conversion_a,0) / nullif(d.deliveries,0) * 100,2) as cvr_a_pct,

    coalesce(conv.conversion_b,0) as conversion_b,
    round(coalesce(conv.conversion_b,0) / nullif(d.deliveries,0) * 100,2) as cvr_b_pct,

    coalesce(conv.conversion_c,0) as conversion_c,
    round(coalesce(conv.conversion_c,0) / nullif(d.deliveries,0) * 100,2) as cvr_c_pct


from delivery_metrics d
left join send_metrics s
  on d.variation_api_id = s.variation_api_id
 and d.campaign_api_id = s.campaign_api_id

left join total_open_metrics t
  on d.variation_api_id = t.variation_api_id
 and d.campaign_api_id = t.campaign_api_id


left join open_metrics_all oa
  on d.variation_api_id = oa.variation_api_id
 and d.campaign_api_id = oa.campaign_api_id

left join click_metrics c
  on d.variation_api_id = c.variation_api_id
 and d.campaign_api_id = c.campaign_api_id

left join unsubscribe_metrics u
  on d.variation_api_id = u.variation_api_id
 and d.campaign_api_id = u.campaign_api_id

left join conversion_metrics conv
  on d.variation_api_id = conv.variation_api_id

left join {{ ref('dim_braze_message_metadata') }} m
  on d.variation_api_id = m.variation_api_id

  left join  {{ source('dev_sweatran', 'DIM_BRAZE_CAMPAIGN_TAGS') }} tag
  on  m.campaign_api_id = tag.campaign_api_id
