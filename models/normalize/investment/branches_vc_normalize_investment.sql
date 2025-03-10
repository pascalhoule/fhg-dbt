 {{  config(alias='branches_vc', database='normalize', schema='investment')  }} 


SELECT 
    REGIONCODE,
    SUBREGIONCODE,
    CODE,
    BRANCH_ID,
    NAME
FROM {{ ref ('branches_vc_clean_investment')  }}