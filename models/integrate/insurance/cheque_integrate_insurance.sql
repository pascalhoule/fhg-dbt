{{  config(alias='cheque', database='integrate', schema='insurance')  }}

SELECT * 
  
from {{ ref ('cheque_normalize_insurance')  }}