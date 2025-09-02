{{  config(alias='brokerassistant_vc', database='clean', schema='insurance')  }} 


SELECT * 
 
FROM {{ source ('insurance_curated', 'brokerassistant_vc')  }}