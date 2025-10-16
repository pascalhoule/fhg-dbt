{{  config(alias='processedcommissions_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'processedcommissions_vc')  }}