{{  config(alias='sponsors_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsors_vc_analyze_investment')  }}