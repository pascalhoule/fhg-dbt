{{  config(alias='itm_policytags_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policytags_fh_report_in_the_mill') }}