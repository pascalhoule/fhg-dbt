{{  config(alias='ad_brokertags_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokertags_fh_advisor_details') }}