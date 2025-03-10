 {{  config(alias='region_vc', database='normalize', schema='investment')  }} 


SELECT 
    SUBREGIONCODE,
    NAME
FROM {{ ref ('region_vc_clean_investment')  }}
