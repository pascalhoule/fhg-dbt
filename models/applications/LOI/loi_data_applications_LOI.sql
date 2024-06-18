 {{  config(alias='loi_data', database='applications', schema='LOI', materialized = "table")  }} 


SELECT DISTINCT
    FA_ODS.ACCOUNT_NUMBER::VARCHAR(255) AS ACCOUNT_NUMBER,
    C.FIRST_NAME::VARCHAR(255) AS CLIENT_FIRST_NAME,
    C.LAST_NAME::VARCHAR(255) AS CLIENT_LAST_NAME,
    REP.REPID::VARCHAR(255) AS REPID,
    REP.FIRST_NAME::VARCHAR(255) AS REP_FIRST_NAME,
    REP.LAST_NAME::VARCHAR(255) AS REP_LAST_NAME,
    RG.NAME::VARCHAR(255) AS BRANCH,
    FA_CUR.ACCOUNTTYPE::VARCHAR(255) AS ACCOUNTTYPE,
    FP.SPONSORID::VARCHAR(255) AS SPONSORID,
    FP.FUNDID::VARCHAR(255) AS FUNDID,
    S.NAME::VARCHAR(255) AS SPONSORNAME,
    SP.ADDRESS::VARCHAR(255) AS STREET,
    SP.CITY::VARCHAR(255) AS CITY,
    SP.STATE_CODE::VARCHAR(255) AS PROVINCE,
    SP.ZIP_CODE::VARCHAR(255) AS POSTCODE,
    SP.MARKET_EMAIL_ADDRESS::VARCHAR(255) AS EMAIL,
    SP.MARKET_FAX::VARCHAR(255) AS FAX
FROM 
    {{ ref('registration_applications_investment') }} R 
LEFT JOIN 
    {{ ref ('fundaccount_applications_investment')  }} FA_ODS 
    ON FA_ODS.ACCOUNT_NUMBER = R.REGISTRATION_NUMBER 
LEFT JOIN 
    {{ ref('clients_applications_investment') }} C ON C.CODE = R.KYC_CODE 
LEFT JOIN 
    {{ ref('representatives_applications_investment') }}  Rep ON Rep.CODE = C.REP_CODE 
LEFT JOIN 
    {{ ref('branches_vc_applications_investment') }} B ON B.CODE = Rep.BRANCH_CODE 
LEFT JOIN 
    {{ ref('region_vc_applications_investment') }} RG ON RG.SUBREGIONCODE = B.SUBREGIONCODE 
LEFT JOIN 
    {{ ref ('fundaccount_vc_applications_investment')  }} FA_CUR ON FA_CUR.FUNDACCOUNTCODE = FA_ODS.CODE
LEFT JOIN 
    {{ ref ('fundproducts_vc_applications_investment')  }} FP ON FP.FUNDPRODUCTCODE = FA_CUR.FUNDPRODUCT_CODE
LEFT JOIN 
    {{ ref ('sponsors_vc_applications_investment')  }} S ON S.SPONSORID = FP.SPONSORID
LEFT JOIN 
    {{ ref ('sponsor_applications_investment')  }} SP ON SP.CODE = S.CODE
WHERE 
    R.CODE IS NOT NULL
    AND R.LEGAL_NAME IS NOT NULL
































