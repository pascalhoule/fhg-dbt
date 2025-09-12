{{  config(alias='itm_brokertags_vc', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokertags_vc_report_in_the_mill') }}