{{  config(alias='sales_calendar_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('calendar_report_sales') }}