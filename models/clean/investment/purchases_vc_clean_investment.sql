{{  config(alias='purchases_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'purchases_vc')  }}