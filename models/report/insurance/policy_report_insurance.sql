{{ config(alias="policy", database="report", schema="insurance") }}


select
    planname as "PLAN NAME",
    senttoicdate as "SENT TO IC DATE",
    maileddate as "MAILED DATE",
    appsource as "APP SOURCE",
    commpremiumamount as "COMM PREMIUM AMOUNT",
    firstownerclientcode as "FIRST OWNER CLIENT CODE",
    issueprovince as "ISSUE PROVINCE",
    settlementdate as "SETTLEMENT DATE",
    lastcommissionprocessdate as "LAST COMMISSION PROCESS DATE",
    premiumamount as "PREMIUM AMOUNT",
    suminsured as "SUMINSURED",
    apptype as "APP TYPE",
    grossamount as "GROSS AMOUNT",
    mgafyoamount as "MGA FYO AMOUNT",
    renewaldate as "RENEWAL DATE",
    carrier as "CARRIER",
    contractdate as "CONTRACT DATE",
    faceamount as "FACE AMOUNT",
    conversionexpirydate as "CONVERSION EXPIRY DATE",
    applicationdate as "APPLICATION DATE",
    createdby as "CREATED BY",
    createddate as "CREATE DDATE",
    expirydate as "EXPIR YDATE",
    annualpremiumamount as "ANNUAL PREMIUM AMOUNT",
    firstinsuredclientcode as "FIRST INSURED CLIENT CODE",
    plantype as "PLANTYPE",
    policynumber as "POLICY NUMBER",
    policy_clientcode as "POLICY CLIENT CODE",
    policy_status_description as "POLICY STATUS DESCRIPTION",
    policy_status_category as "POLICY STATUS CATEGORY",
    planid as "PLANID",
    policycode as "POLICY CODE",
    beneficiaries as "BENEFICIARIES"

from {{ ref("policy_analyze_insurance") }}