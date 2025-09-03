 {{  config(alias='marketingoffice_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'marketingoffice_vc')  }}