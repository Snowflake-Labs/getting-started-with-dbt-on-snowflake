with source as (
        select * from {{ source('dev_sweatran', 'DIM_BRAZE_CAMPAIGN_TAGS') }}
  ),
  renamed as (
      select
          {{ adapter.quote("CAMPAIGN_API_ID") }},
        {{ adapter.quote("TAGS") }}

      from source
  )
  select * from renamed
    