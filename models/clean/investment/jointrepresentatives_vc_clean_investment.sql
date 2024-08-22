{{  config(alias='jointrepresentatives_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'jointrepresentatives_vc')  }}