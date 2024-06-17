{{  config(alias='cheque', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  


from {{ ref ('cheque_clean_insurance')  }}