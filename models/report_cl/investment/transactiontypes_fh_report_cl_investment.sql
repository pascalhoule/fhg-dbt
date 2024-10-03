{{  config( alias='transactiontypes_fh', database='report', schema='investment' )  }}

SELECT * 
  


from {{ ref ('transactiontypes_fh_analyze_investment')  }}