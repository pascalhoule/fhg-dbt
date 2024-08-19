{{  config( alias='transactiontypes_fh', database='analyze', schema='investment' )  }}

SELECT * 
  


from {{ ref ('transactiontypes_fh_integrate_investment')  }}