{{  config(alias='ad_employee_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_employee_fh_for_share_clean') }}