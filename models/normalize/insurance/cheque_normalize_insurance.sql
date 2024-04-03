{{  config(alias='cheque', database='normalize', schema='insurance')  }}

SELECT * 
  
from {{ ref ('cheque_clean_insurance')  }}