{{  config(alias='cheque', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  


from {{ ref ('cheque_clean_insurance')  }}