{{  config( alias='transactiontypes_fh', database='agt_comm', schema='investment' )  }}

SELECT * 
  


from {{ ref ('transactiontypes_fh_analyze_investment')  }}