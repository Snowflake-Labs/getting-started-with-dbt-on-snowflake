SELECT *
FROM {{ source('tb_101', 'LOCATION') }}
