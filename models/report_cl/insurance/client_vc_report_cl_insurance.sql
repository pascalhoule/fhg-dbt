{{  config(alias='client_vc', database='report_cl', schema='insurance')  }} 
 

SELECT
    *
  
from {{ ref ('client_vc_analyze_insurance')  }}