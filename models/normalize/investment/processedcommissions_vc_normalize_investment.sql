 {{  config(alias='processedcommissions_vc', database='normalize', schema='investment')  }} 


SELECT 
    *
FROM {{ ref ('processedcommissions_vc_clean_investment')  }}