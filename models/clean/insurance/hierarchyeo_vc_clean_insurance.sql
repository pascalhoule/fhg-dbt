 {{  config(alias='hierarchyeo_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'hierarchyeo_vc')  }}