 {{  config(alias='processedtrailfees_vc', database='normalize', schema='investment')  }} 


SELECT 
    *
FROM {{ ref ('processedtrailfees_vc_clean_investment')  }}