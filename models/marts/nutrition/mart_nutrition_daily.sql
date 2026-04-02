select
    date_trunc('day', logged_at) as log_date,
    user_id,
    sum(calories) as total_calories,
    sum(protein_g) as total_protein_g,
    sum(carbs_g) as total_carbs_g,
    sum(fat_g) as total_fat_g,
    count(distinct meal_type) as meal_types_logged
from {{ ref('fct_food_logs') }}
group by 1, 2
