 {{  config(alias='aua_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_analyze_investment')  }}