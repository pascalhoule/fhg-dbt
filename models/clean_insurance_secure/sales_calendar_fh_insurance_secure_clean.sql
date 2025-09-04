{{  config(alias='sales_calendar_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sales_calendar_fh_for_share_clean') }}