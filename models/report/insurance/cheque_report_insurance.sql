{{  config(alias='cheque', database='report', schema='insurance')  }}

SELECT * 
  
from {{ ref ('cheque_analyze_insurance')  }}