{{  config(alias='itm_policy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_fh_report_in_the_mill') }}