{{  config(alias='sales_brokeradvanced_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeradvanced_report_sales') }}