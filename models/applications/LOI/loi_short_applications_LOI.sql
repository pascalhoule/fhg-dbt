 {{  config(alias='loi_short', database='applications', schema='LOI', materialized = "view")  }} 

SELECT DISTINCT
    FA_ODS.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
    R.LEGAL_NAME AS CLIENTNAME,
    REP.REPID,
    REP.FIRST_NAME AS REP_FIRST_NAME,
    REP.LAST_NAME AS REP_LAST_NAME,
    RG.NAME AS BRANCH,
    FA_CUR.ACCOUNTTYPE,
    FP.SPONSORID,
    FP.FUNDID,
    S.NAME AS SPONSORNAME,
    SP.ADDRESS AS STREET,
    SP.CITY,
    SP.STATE_CODE AS PROVINCE,
    SP.ZIP_CODE AS POSTCODE,
    SP.MARKET_EMAIL_ADDRESS AS EMAIL,
    SP.MARKET_FAX AS FAX
FROM 
    {{ ref ('registration_applications_investment')  }} R
LEFT JOIN 
    {{ ref ('fundaccount_applications_investment')  }} FA_ODS ON FA_ODS.ACCOUNT_NUMBER = R.REGISTRATION_NUMBER
LEFT JOIN 
    {{ ref ('transactions_vc_applications_investment')  }} T ON T.FUNDACCOUNT_CODE = FA_ODS.CODE
LEFT JOIN 
    {{ ref ('representatives_vc_applications_investment')  }}  Rep ON Rep.CODE = T.CLIENTREPCODE
LEFT JOIN 
    {{ ref ('branches_vc_applications_investment')  }} B ON B.CODE = Rep.BRANCH_CODE
LEFT JOIN 
    {{ ref ('region_vc_applications_investment')  }}  RG ON RG.SUBREGIONCODE = B.SUBREGIONCODE
LEFT JOIN 
    {{ ref ('fundaccount_vc_applications_investment')  }} FA_CUR ON FA_CUR.FUNDACCOUNTCODE = T.FUNDACCOUNT_CODE
LEFT JOIN 
    {{ ref ('fundproducts_vc_applications_investment')  }}  FP ON FP.FUNDPRODUCTCODE = FA_CUR.FUNDPRODUCT_CODE
LEFT JOIN 
    {{ ref ('sponsors_vc_applications_investment')  }}  S ON S.SPONSORID = FP.SPONSORID
LEFT JOIN 
    {{ ref ('sponsor_applications_investment')  }}  SP ON SP.CODE = S.CODE
WHERE 
    R.CODE IS NOT NULL
    AND R.LEGAL_NAME IS NOT NULL