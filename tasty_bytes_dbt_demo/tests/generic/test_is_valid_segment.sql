{% test is_valid_loyalty_segment(model, column_name, sales_column) %}

select *
from {{ model }}
where 
    -- Logic 1: VIPs must have > 500
    ({{ column_name }} = 'VIP' and {{ sales_column }} <= 500)
    
    -- Logic 2: Regulars must have > 100
    or ({{ column_name }} = 'Regular' and {{ sales_column }} <= 100)
    
    -- Logic 3: Catch any nulls in the segment
    or ({{ column_name }} is null)

{% endtest %}