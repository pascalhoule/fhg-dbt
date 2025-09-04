{{  config(alias='users_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'users_vc')  }}