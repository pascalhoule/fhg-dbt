{{  config( alias='transactiontypes_fh', database='integrate', schema='investment' )  }}

SELECT * 
  


from {{ ref ('transactiontypes_fh_normalize_investment')  }}