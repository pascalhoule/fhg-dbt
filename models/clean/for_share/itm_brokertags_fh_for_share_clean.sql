{{  config(alias='itm_brokertags_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokertags_fh_report_in_the_mill') }}