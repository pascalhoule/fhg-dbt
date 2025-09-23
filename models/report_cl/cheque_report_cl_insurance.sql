{{  config(alias='cheque', database='report_cl', schema='insurance')  }}

SELECT * 
  
from {{ ref ('cheque_analyze_insurance')  }}