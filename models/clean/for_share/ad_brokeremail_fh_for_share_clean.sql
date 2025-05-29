{{  config(alias='ad_brokeremail_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeremail_fh_report_advisor_details') }}