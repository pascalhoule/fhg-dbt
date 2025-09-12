{{  config(alias='itm_broker_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('broker_fh_report_in_the_mill') }}