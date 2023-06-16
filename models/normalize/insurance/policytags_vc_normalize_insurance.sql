 {{  config(alias='policytags_vc', database='normalize', schema='insurance')  }} 


SELECT
    source,
    policycode,
    ARRAY_AGG(tagname) AS tagname_array,
    ARRAY_TO_STRING(ARRAY_AGG(tagname), ' ; ') AS tagname_combined

from {{ ref ('policytags_vc_clean_insurance')  }}
GROUP BY source, policycode