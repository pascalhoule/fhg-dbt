{{  config(alias='fundaccount', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_analyze_investment') }}