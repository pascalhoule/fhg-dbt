{{  config(alias='loi_tracking_sort', database='applications', schema='LOI', materialized = "view")  }} 

SELECT
    *
FROM
    {{ source('app_tables', 'loi_tracking') }}
ORDER BY
    DATE_CREATED DESC