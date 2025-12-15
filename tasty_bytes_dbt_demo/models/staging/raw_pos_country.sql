-- Add a comment here so I can push the branch
SELECT *
FROM {{ source('tb_101', 'COUNTRY') }}
