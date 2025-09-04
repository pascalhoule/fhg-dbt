 {{  config(alias='hierarchycontract_vc', database='clean', schema='insurance')  }} 


SELECT * 
 

FROM {{ source ('insurance_curated', 'hierarchycontract_vc')  }}