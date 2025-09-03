{{  config(alias='trailingfees_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'trailingfees_vc')  }}