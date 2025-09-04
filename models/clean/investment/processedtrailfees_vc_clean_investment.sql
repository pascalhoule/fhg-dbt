{{  config(alias='processedtrailfees_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'processedtrailfees_vc')  }}