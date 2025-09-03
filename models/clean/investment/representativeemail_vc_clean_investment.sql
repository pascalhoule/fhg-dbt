{{  config(alias='representativeemail_vc', database='clean', schema='investment')  }} 

SELECT * 


from {{ source ('investment_curated', 'representativeemail_vc')  }}