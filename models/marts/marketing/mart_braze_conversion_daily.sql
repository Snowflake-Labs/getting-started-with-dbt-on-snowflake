select
    event_date_utc,
    variation_api_id,
    canvas_step_api_id,
    conversion_behavior_index,

    sum(conversion) as conversions,
    count(distinct case when conversion = 1 then contact_id end) as unique_converters

from {{ ref('fct_braze_conversion') }}
group by 1,2,3,4