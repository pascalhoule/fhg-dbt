 {{  config(alias='branches_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('branches_vc_analyze_investment')  }}