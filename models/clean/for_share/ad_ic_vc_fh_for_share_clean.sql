{{  config(alias='ad_ic_vc_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('ic_vc_integrate_insurance') }}