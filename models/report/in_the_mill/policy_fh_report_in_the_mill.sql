{{			
    config (			
        materialized="view",			
        alias='policy_fh', 			
        database='report', 			
        schema = 'in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["in_the_mill"]			
    )			
}}

    SELECT
        FH_POLICYCATEGORY,
        POLICYCODE,
        FH_SERVICINGAGTCODE,
        FH_SERVICINGAGTSPLIT,
        FH_COMMISSIONINGAGTCODE,
        FH_COMMISSIONINGAGTSPLIT,
        FH_CARRIERENG,
        FH_CARRIERFR,
        POLICYNUMBER,
        PLANID,
        FH_PLANTYPE,
        FH_PLANNAMEENG,
        FH_PLANNAMEFR,
        PREMIUMAMOUNT,
        CREATEDDATE,
        FH_STATUSCODE,
        FH_STATUSNAMEENG,
        FH_STATUSNAMEFR,
        FH_STATUSCATEGORY,
        APPCOUNT,
        FH_FYCSERVAMT,
        FH_FYCCOMMAMT,
        ISSUEPROVINCE,
        FH_APPSOURCE,
        FH_APPTYPE,
        FH_SETTLEMENTDATE,
        FH_STARTDATE,
        FH_PREMIUM,
        FH_PREM_COMMWGT,
        FH_PREM_SERVWGT
    FROM
        {{ ref('policy_fh_report_insurance') }}