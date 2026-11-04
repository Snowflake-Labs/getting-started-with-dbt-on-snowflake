-- What tables exist?
SHOW TABLES IN SCHEMA tb_101.raw_pos;

-- What is the scale of data? 
SELECT COUNT(*) FROM tb_101.raw_pos.order_header;

-- Understand a query that might be used in a mart sankara
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw.customer_loyalty cl
JOIN tb_101.raw.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;