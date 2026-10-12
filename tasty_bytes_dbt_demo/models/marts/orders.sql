SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total,
    'test column' as test
FROM {{ ref('raw_pos_order_detail') }} od
JOIN {{ ref('raw_pos_order_header') }} oh
    ON od.order_id = oh.order_id
JOIN {{ ref('raw_pos_truck') }} t
    ON oh.truck_id = t.truck_id
JOIN {{ ref('raw_pos_menu') }} m
    ON od.menu_item_id = m.menu_item_id
JOIN {{ ref('raw_pos_franchise') }} f
    ON t.franchise_id = f.franchise_id
JOIN {{ ref('raw_pos_location') }} l
    ON oh.location_id = l.location_id
LEFT JOIN {{ ref('raw_customer_customer_loyalty') }} cl
    ON oh.customer_id = cl.customer_id
