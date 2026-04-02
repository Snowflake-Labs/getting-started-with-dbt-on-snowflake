with campaign_variations as (

    select distinct
        'CAMPAIGN' as source_type,
        source_id,
        campaign_api_id,
        name,
        conversion_behaviors,
        event_time_utc,
        v1.value:api_id::string as variation_api_id,
        v1.value:name::string as variation_name,
        v1.value:id::string as variation_id
    from {{ ref('stg_braze__campaign_changelogs') }}
    join lateral flatten(input => actions) v
    join lateral flatten(input => v.value:message_variations) v1

),

canvas_variations as (

    select distinct
        'CANVAS' as source_type,
        source_id,
        campaign_api_id,
        name,
        conversion_behaviors,
        event_time_utc,
        v.value:api_id::string as variation_api_id,
        v.value:name::string as variation_name,
        v.value:id::string as variation_id
    from {{ ref('stg_braze__canvas_changelogs') }}
    join lateral flatten(input => variations) v

),

unioned as (

    select * from campaign_variations
    union all
    select * from canvas_variations

),

ranked as (

    select
        *,
        row_number() over (
            partition by variation_api_id
            order by event_time_utc desc
        ) as rn
    from unioned

)

select
    source_type,
    source_id,
    campaign_api_id,
    name,
    conversion_behaviors,
    event_time_utc,
    variation_api_id,
    variation_name,
    variation_id
from ranked
where rn = 1