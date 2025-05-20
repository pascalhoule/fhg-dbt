{{  config(alias='ad_employee_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('employee_fh_report_advisor_details') }}