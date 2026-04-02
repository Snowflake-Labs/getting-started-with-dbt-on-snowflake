with source as (
    select * from {{ source('nutrition', 'food_logs') }}
),

renamed as (
    select
        log_id,
        user_id,
        food_item_id,
        meal_type,
        logged_at,
        quantity_grams,
        calories,
        protein_g,
        carbs_g,
        fat_g
    from source
)

select * from renamed
