{{  config(alias='__FHFORMAT', database='normalize', schema='insurance')  }} 

 SELECT
        FH_POLICYCATEGORY,
        POLICYCODE,
        POLICYGROUPCODE,
        FH_SERVICINGAGTCODE,
        SPLIT_PART(CARRIER, ' / ', 1) AS FH_CARRIERENG,
        SPLIT_PART(CARRIER, ' / ', 2) AS FH_CARRIERFR,
        POLICYNUMBER,
        PLANID,
        CASE
            WHEN PLANTYPE = 'HealthSickness' THEN 'H&S'
            WHEN PLANTYPE = 'SERVICE' THEN 'Service'
            ELSE PLANTYPE
        END AS FH_PLANTYPE,
        SPLIT_PART(PLANNAME, '/', 1) AS FH_PLANNAMEENG,
        SPLIT_PART(PLANNAME, '/', 2) AS FH_PLANNAMEFR,
        PREMIUMAMOUNT,
        ANNUALPREMIUMAMOUNT,
        COMMPREMIUMAMOUNT,
        PAYMENTMODE,
        FACEAMOUNT,
        CREATEDBY,
        CREATEDDATE,
        APPLICATIONDATE,
        CONTRACTDATE,
        SETTLEMENTDATE,
        EXPIRYDATE,
        RENEWALDATE,
        SENTTOICDATE,
        MAILEDDATE,
        FH_FINPOSTDATE,
        STATUS AS FH_STATUSCODE,
        STATUS.ENGLISHDESCRIPTION AS FH_STATUSNAMEENG,
        STATUS.FRENCHDESCRIPTION AS FH_STATUSNAMEFR,
        STATUS.CATEGORY AS FH_STATUSCATEGORY,
        FH_SERVICINGAGTSPLIT,
        APPCOUNT,
        FYCAMOUNT,
        MGAFYOAMOUNT,
        ISSUEPROVINCE,
        CASE
            WHEN APPSOURCE = 'manual' THEN 'Manual'
            WHEN APPSOURCE = 'feed' THEN 'Feed'
            ELSE APPSOURCE
        END AS FH_APPSOURCE,
        CASE
            WHEN APPTYPE = 'paper' THEN 'Paper'
            WHEN APPTYPE = 'eapp' THEN 'Eapp'
            ELSE APPTYPE
        END AS FH_APPTYPE,
        LASTCOMMISSIONPROCESSDATE,
        FIRSTOWNERCLIENTCODE,
        FIRSTINSUREDCLIENTCODE,
        ISMAINCOVERAGE,
        FH_SETTLEMENTDATE,
        LEAST(APPLICATIONDATE, CREATEDDATE) AS FH_STARTDATE,
        CASE
            WHEN COMMPREMIUMAMOUNT is not null THEN IFF(
                COMMPREMIUMAMOUNT = 0,
                ANNUALPREMIUMAMOUNT,
                COMMPREMIUMAMOUNT
            )
            WHEN COMMPREMIUMAMOUNT is null THEN 0
        END AS FH_PREMIUM
    FROM
        {{ ref ('__POSTDATE_insurance') }} POL
        JOIN {{ ref ('policystatus_vc_normalize_insurance') }} STATUS ON POL.STATUS = STATUS.CODE