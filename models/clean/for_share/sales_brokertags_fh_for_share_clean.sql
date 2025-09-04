{{  config(alias='sales_brokertags_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokertags_report_sales') }}