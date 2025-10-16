 {{  config(alias='entitytags_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'entitytags_vc')  }}