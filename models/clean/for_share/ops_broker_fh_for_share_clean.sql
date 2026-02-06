{{  config(alias='ops_broker_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('broker_fh_integrate_insurance') }}