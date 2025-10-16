{{  config(alias='itm_brokeradvanced', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeradvanced_report_in_the_mill') }}