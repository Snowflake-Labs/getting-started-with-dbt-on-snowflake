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
from {{ ref('stg_nutrition__food_logs') }}
